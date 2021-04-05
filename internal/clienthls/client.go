package clienthls

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aler9/gortsplib"
	"github.com/aler9/gortsplib/pkg/headers"
	"github.com/aler9/gortsplib/pkg/ringbuffer"
	"github.com/aler9/gortsplib/pkg/rtpaac"
	"github.com/aler9/gortsplib/pkg/rtph264"
	"github.com/asticode/go-astits"

	"github.com/aler9/rtsp-simple-server/internal/aac"
	"github.com/aler9/rtsp-simple-server/internal/client"
	"github.com/aler9/rtsp-simple-server/internal/h264"
	"github.com/aler9/rtsp-simple-server/internal/logger"
	"github.com/aler9/rtsp-simple-server/internal/stats"
)

const (
	pauseAfterAuthError = 2 * time.Second
)

type trackIDPayloadPair struct {
	trackID int
	buf     []byte
}

// PathMan is implemented by pathman.PathMan.
type PathMan interface {
	OnClientSetupPlay(client.SetupPlayReq)
}

// Parent is implemented by clientman.ClientMan.
type Parent interface {
	Log(logger.Level, string, ...interface{})
	OnClientClose(client.Client)
}

// Client is a HLS client.
type Client struct {
	readBufferCount int
	wg              *sync.WaitGroup
	stats           *stats.Stats
	pathName        string
	pathMan         PathMan
	parent          Parent

	ringBuffer *ringbuffer.RingBuffer

	// in
	terminate chan struct{}
}

// New allocates a Client.
func New(
	readBufferCount int,
	wg *sync.WaitGroup,
	stats *stats.Stats,
	pathName string,
	pathMan PathMan,
	parent Parent) *Client {

	c := &Client{
		readBufferCount: readBufferCount,
		wg:              wg,
		stats:           stats,
		pathName:        pathName,
		pathMan:         pathMan,
		parent:          parent,
		terminate:       make(chan struct{}),
	}

	atomic.AddInt64(c.stats.CountClients, 1)
	c.log(logger.Info, "connected (HLS)")

	c.wg.Add(1)
	go c.run()

	return c
}

// Close closes a Client.
func (c *Client) Close() {
	atomic.AddInt64(c.stats.CountClients, -1)
	close(c.terminate)
}

// IsClient implements client.Client.
func (c *Client) IsClient() {}

// IsSource implements path.source.
func (c *Client) IsSource() {}

func (c *Client) log(level logger.Level, format string, args ...interface{}) {
	c.parent.Log(level, "[client hls/%s] "+format, append([]interface{}{c.pathName}, args...)...)
}

func (c *Client) PathName() string {
	return c.pathName
}

func (c *Client) run() {
	defer c.wg.Done()
	defer c.log(logger.Info, "disconnected")

	var path client.Path
	var tracks gortsplib.Tracks

	var videoTrack *gortsplib.Track
	var h264SPS []byte
	var h264PPS []byte
	var h264Decoder *rtph264.Decoder
	var audioTrack *gortsplib.Track
	var aacConfig rtpaac.MPEG4AudioConfig
	var aacDecoder *rtpaac.Decoder

	err := func() error {
		resc := make(chan client.SetupPlayRes)
		c.pathMan.OnClientSetupPlay(client.SetupPlayReq{c, c.pathName, nil, resc}) //nolint:govet
		res := <-resc

		if res.Err != nil {
			if _, ok := res.Err.(client.ErrAuthCritical); ok {
				// wait some seconds to stop brute force attacks
				select {
				case <-time.After(pauseAfterAuthError):
				case <-c.terminate:
				}
			}
			return res.Err
		}

		path = res.Path
		tracks = res.Tracks

		return nil
	}()
	if err != nil {
		c.log(logger.Info, "ERR: %s", err)

		c.parent.OnClientClose(c)
		<-c.terminate
		return
	}

	err = func() error {
		for i, t := range tracks {
			if t.IsH264() {
				if videoTrack != nil {
					return fmt.Errorf("can't read track %d with HLS: too many tracks", i+1)
				}
				videoTrack = t

				var err error
				h264SPS, h264PPS, err = t.ExtractDataH264()
				if err != nil {
					return err
				}

				h264Decoder = rtph264.NewDecoder()

			} else if t.IsAAC() {
				if audioTrack != nil {
					return fmt.Errorf("can't read track %d with HLS: too many tracks", i+1)
				}
				audioTrack = t

				byts, err := t.ExtractDataAAC()
				if err != nil {
					return err
				}

				err = aacConfig.Decode(byts)
				if err != nil {
					return err
				}

				aacDecoder = rtpaac.NewDecoder(aacConfig.SampleRate)
			}
		}

		if videoTrack == nil && audioTrack == nil {
			return fmt.Errorf("unable to find a video or audio track")
		}

		c.ringBuffer = ringbuffer.New(uint64(c.readBufferCount))

		return nil
	}()
	if err != nil {
		c.log(logger.Info, "ERR: %s", err)

		res := make(chan struct{})
		path.OnClientRemove(client.RemoveReq{c, res}) //nolint:govet
		<-res
		path = nil

		c.parent.OnClientClose(c)
		<-c.terminate
		return
	}

	resc := make(chan client.PlayRes)
	path.OnClientPlay(client.PlayReq{c, resc}) //nolint:govet
	<-resc

	c.log(logger.Info, "is reading from path '%s'", c.pathName)

	f, err := os.Create("/s/test.ts")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	w := bufio.NewWriter(f)

	mux := astits.NewMuxer(context.Background(), w,
		astits.MuxerOptTablesRetransmitPeriod(40))

	if videoTrack != nil {
		mux.AddElementaryStream(astits.PMTElementaryStream{
			ElementaryPID: 256,
			StreamType:    astits.StreamTypeH264Video,
		})
	}

	if audioTrack != nil {
		mux.AddElementaryStream(astits.PMTElementaryStream{
			ElementaryPID: 257,
			StreamType:    astits.StreamTypeAACAudio,
		})
	}

	mux.SetPCRPID(256)

	writerDone := make(chan error)
	go func() {
		writerDone <- func() error {
			videoInitialized := false
			var videoStartDTS time.Time
			//var videoLastDTS time.Duration
			var videoStartPTS time.Duration
			var videoBuf [][]byte

			audioInitialized := false
			var audioStartPTS time.Duration

			for {
				data, ok := c.ringBuffer.Pull()
				if !ok {
					return fmt.Errorf("terminated")
				}
				pair := data.(trackIDPayloadPair)

				if videoTrack != nil && pair.trackID == videoTrack.ID {
					nalus, pts, err := h264Decoder.Decode(pair.buf)
					if err != nil {
						if err != rtph264.ErrMorePacketsNeeded {
							c.log(logger.Warn, "unable to decode video track: %v", err)
						}
						continue
					}

					if !videoInitialized {
						videoInitialized = true
						videoStartDTS = time.Now()
						videoStartPTS = pts
					}

					for _, nalu := range nalus {
						// remove SPS, PPS, AUD
						typ := h264.NALUType(nalu[0] & 0x1F)
						switch typ {
						case h264.NALUTypeSPS, h264.NALUTypePPS, h264.NALUTypeAccessUnitDelimiter:
							continue
						}

						// add SPS and PPS before IDR
						if typ == h264.NALUTypeIDR {
							videoBuf = append(videoBuf, h264SPS)
							videoBuf = append(videoBuf, h264PPS)
						}

						videoBuf = append(videoBuf, nalu)
					}

					// RTP marker means that all the NALUs with the same PTS have been received.
					// send them together.
					marker := (pair.buf[1] >> 7 & 0x1) > 0
					if marker {
						enc, err := h264.EncodeAnnexB(videoBuf)
						if err != nil {
							return err
						}

						dts := time.Now().Sub(videoStartDTS)

						// avoid duplicate DTS
						// (RTMP has a resolution of 1ms)
						/*if int64(dts/time.Millisecond) <= (int64(videoLastDTS / time.Millisecond)) {
							dts = videoLastDTS + time.Millisecond
						}
						videoLastDTS = dts*/

						tsdts := int64(dts.Seconds() * 90000)
						tspts := int64((2*time.Second + pts - videoStartPTS).Seconds() * 90000)

						_, err = mux.WriteData(&astits.MuxerData{
							PID: 256,
							PES: &astits.PESData{
								Header: &astits.PESHeader{
									OptionalHeader: &astits.PESOptionalHeader{
										MarkerBits:      2,
										PTSDTSIndicator: astits.PTSDTSIndicatorBothPresent,
										DTS:             &astits.ClockReference{Base: tsdts},
										PTS:             &astits.ClockReference{Base: tspts},
									},
									StreamID: 224, // = video
								},
								Data: enc,
							},
						})
						if err != nil {
							return err
						}

						videoBuf = nil
					}

				} else if audioTrack != nil && pair.trackID == audioTrack.ID {
					aus, pts, err := aacDecoder.Decode(pair.buf)
					if err != nil {
						if err != rtpaac.ErrMorePacketsNeeded {
							c.log(logger.Warn, "unable to decode audio track: %v", err)
						}
						continue
					}

					if !audioInitialized {
						audioInitialized = true
						audioStartPTS = pts
					}

					for i, au := range aus {
						auPTS := (pts) + time.Duration(i)*1000*time.Second/time.Duration(aacConfig.SampleRate)

						adtsPkt, err := aac.EncodeADTS([]*aac.ADTSPacket{
							{
								SampleRate:   aacConfig.SampleRate,
								ChannelCount: aacConfig.ChannelCount,
								Frame:        au,
							},
						})
						if err != nil {
							c.log(logger.Warn, "unable to encode ADTS: %v", err)
							continue
						}

						dts := time.Now().Sub(videoStartDTS)

						tsdts := int64(dts.Seconds() * 90000)
						tspts := int64((2*time.Second + auPTS - audioStartPTS).Seconds() * 90000)

						_, err = mux.WriteData(&astits.MuxerData{
							PID: 257,
							PES: &astits.PESData{
								Header: &astits.PESHeader{
									OptionalHeader: &astits.PESOptionalHeader{
										MarkerBits:      2,
										PTSDTSIndicator: astits.PTSDTSIndicatorBothPresent,
										DTS:             &astits.ClockReference{Base: tsdts},
										PTS:             &astits.ClockReference{Base: tspts},
									},
									PacketLength: uint16(len(adtsPkt) + 8),
									StreamID:     192, // = audio
								},
								Data: adtsPkt,
							},
						})
						if err != nil {
							return err
						}
					}
				}
			}
		}()
	}()

	select {
	case err := <-writerDone:
		c.log(logger.Info, "ERR: %s", err)

		res := make(chan struct{})
		path.OnClientRemove(client.RemoveReq{c, res}) //nolint:govet
		<-res
		path = nil

		c.parent.OnClientClose(c)
		<-c.terminate

	case <-c.terminate:
		res := make(chan struct{})
		path.OnClientRemove(client.RemoveReq{c, res}) //nolint:govet
		<-res

		c.ringBuffer.Close()
		<-writerDone
		path = nil
	}
}

// Authenticate performs an authentication.
func (c *Client) Authenticate(authMethods []headers.AuthMethod,
	pathName string, ips []interface{},
	user string, pass string, req interface{}) error {
	return nil
}

func (c *Client) OnFrame(trackID int, streamType gortsplib.StreamType, payload []byte) {
	if streamType == gortsplib.StreamTypeRTP {
		c.ringBuffer.Push(trackIDPayloadPair{trackID, payload})
	}
}
