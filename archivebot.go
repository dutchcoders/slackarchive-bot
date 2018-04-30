package archivebot

import (
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path"
	"runtime"
	"time"

	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	elastic "gopkg.in/olivere/elastic.v5"

	"github.com/gorilla/websocket"
	"github.com/nlopes/slack"
	logging "github.com/op/go-logging"

	config "github.com/dutchcoders/slackarchive-bot/config"
	models "github.com/dutchcoders/slackarchive-bot/models"
	utils "github.com/dutchcoders/slackarchive-bot/utils"
	// elastigo "github.com/mattbaird/elastigo/lib"

	durable "github.com/dutchcoders/durable"

	log2 "log"

	"net/http"
	"net/url"
)

const (
	// Time allowed to write a message to the peer.
	writeWait = 10 * time.Second

	// Time allowed to read the next pong message from the peer.
	pongWait = 60 * time.Second

	// Send pings to peer with this period. Must be less than pongWait.
	pingPeriod = 2 * time.Second

	// Maximum message size allowed from peer.
	maxMessageSize = 16384
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

var log = logging.MustGetLogger("archivebot")

type archiveBot struct {
	session *mgo.Session
	es      *elastic.Client

	// we are using both an indexer and message chan
	// because all messages are being persisted
	indexerChan chan interface{}
	messageChan chan interface{}

	archivers map[string]*archiveClient
	config    *config.Config
	work      chan func()
}

func New(conf *config.Config) *archiveBot {
	session, err := mgo.Dial(conf.DSN)
	if err != nil {
		log.Error("Error opening database: %s", err.Error())
		return nil
	}

	session.SetMode(mgo.Monotonic, true)

	es, err := elastic.NewClient(elastic.SetURL(conf.ElasticSearch.Host), elastic.SetSniff(false))
	if err != nil {
		panic(err)
	}

	writer := make(chan interface{})

	datadir := path.Join(conf.DataDir, fmt.Sprintf("slackarchive-%s", time.Now().Format("20060102150405")))

	if err := os.Mkdir(datadir, 0755); err != nil {
		log.Error("Could not create data dir %s: %s", datadir, err)
		return nil
	}

	c := durable.Channel(writer, &durable.Config{
		Name:            "",
		DataPath:        datadir,
		MaxBytesPerFile: 1048576,
		MinMsgSize:      0,
		MaxMsgSize:      16384,
		SyncEvery:       10,
		SyncTimeout:     time.Second * 10,
		Logger:          log2.New(os.Stdout, "", 0),
	})

	return &archiveBot{
		config:      conf,
		session:     session,
		es:          es,
		work:        make(chan func()),
		messageChan: writer,
		indexerChan: c,
		archivers:   map[string]*archiveClient{},
	}

}

func hash(s string) string {
	h := sha1.New()
	h.Write([]byte(s))
	return hex.EncodeToString(h.Sum(nil))
}

func basicAuth(username, password string) string {
	auth := username + ":" + password
	return base64.StdEncoding.EncodeToString([]byte(auth))
}

// alternative messagehandler using websockets
func (ab *archiveBot) runMessageHandler2() {
	u, err := url.Parse(ab.config.ApiURL)
	if err != nil {
		log.Error("Could not parse url %s: %s", ab.config.ApiURL, err.Error())
		return
	}

	// basic authentication or certificates?
	dialer := websocket.Dialer{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}

	dialer.TLSClientConfig = &tls.Config{}

	go func() {
		count := 0

		for {
			func() {
				requestHeader := http.Header{}
				requestHeader.Set("Authorization", fmt.Sprintf("Token %s", hash(ab.config.ApiToken)))

				c, _, err := dialer.Dial(u.String(), requestHeader)
				if err != nil {
					log.Error("Could not connect to (%s): ", u.String(), err.Error())
					return
				}

				defer c.Close()

				ticker := time.NewTicker(pingPeriod)
				defer func() {
					ticker.Stop()
					c.Close()
				}()

				c.SetReadLimit(maxMessageSize)
				c.SetReadDeadline(time.Now().Add(pongWait))
				c.SetPongHandler(func(string) error { c.SetReadDeadline(time.Now().Add(pongWait)); return nil })

				log.Info("Connected to api socket.")

				ch := make(chan bool)

				// read messages
				go func(c *websocket.Conn) {
					defer func() {
						ch <- true
					}()

					for {
						if _, _, err := c.ReadMessage(); err == nil {
						} else if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway) {
							log.Error("Connection closed unexpectedly: %s", err.Error())
							break
						} else {
							log.Info("Error reading message: %s", err.Error())
							break
						}
					}

				}(c)

				// write messages
				for {
					select {
					case <-ch:
						return
					case msg := <-ab.indexerChan:
						if data, err := json.Marshal(msg); err != nil {
							log.Error("Could not serialize message: %s", err.Error())
							continue
						} else if err := c.WriteMessage(websocket.TextMessage, data); err != nil {
							log.Error("Could not write message: %s", err.Error())
							return
						}

						count++

						if count%100 == 0 {
							log.Info("Number of messages harvested: %d.", count)
						}
					case <-ticker.C:
						c.SetWriteDeadline(time.Now().Add(writeWait))
						if err := c.WriteMessage(websocket.PingMessage, []byte{}); err != nil {
							log.Error("writePump error: %s", err.Error())
							return
						}
					}
				}
			}()

			log.Info("Connection lost. Reconnecting in 5 seconds.")
			time.Sleep(time.Second * 5)
		}
	}()
}

type archiveClient struct {
	*slack.Client
	ab       *archiveBot
	token    string
	team     *slack.TeamInfo
	channels []slack.Channel
}

func (ac *archiveClient) Sync() error {
	for {
		ac.ab.work <- func() func() {
			return func() {
				session := ac.ab.session.Copy()
				defer session.Close()

				db := Database(session)

				log.Info("Syncing team")

				team, err := ac.GetTeamInfo()
				if err != nil {
					log.Error("GetTeamInfo: %s", err.Error())
					return
				}

				// update team
				ac.team = team

				// update team
				t := models.Team{}
				t.Token = ac.token

				if err := utils.Merge(&t, *team); err != nil {
					log.Error("Error merging team(%s): %s", team.ID, err.Error())
					return
				}

				if _, err = db.Teams.UpsertId(t.ID, bson.M{
					"$set": &t,
				}); err != nil {
					log.Error("Error upserting team(%s): %s", team.ID, err.Error())
					return
				}

				log.Info("Syncing team finished")
			}
		}()

		ac.ab.work <- func(teamID string) func() {
			return func() {
				session := ac.ab.session.Copy()
				defer session.Close()

				db := Database(session)

				log.Info("Syncing users(%s)", teamID)

				count := 0

				users, err := ac.GetUsers()
				for _, user := range users {
					u := models.User{}
					if err := utils.Merge(&u, user); err != nil {
						log.Error("Error merging user(%s): %s", user.ID, err.Error())
						continue
					}

					u.Team = ac.team.ID

					if _, err = db.Users.UpsertId(u.ID, &u); err != nil {
						log.Error("Error upserting user(%s): %s", user.ID, err.Error())
						continue
					}

					count += 1
				}

				log.Info("Syncing users completed(%s): %#v %d", teamID, err, count)
			}
		}(ac.team.ID)

		ac.ab.work <- func(teamID string) func() {
			return func() {
				session := ac.ab.session.Copy()
				defer session.Close()

				db := Database(session)

				log.Info("Syncing channels (%s)", teamID)

				count := 0

				channels, err := ac.GetChannels(false)
				if err != nil {
					log.Error("Error syncing channels (%s): %s", teamID, err.Error())
					return
				}

				ac.channels = channels

				for _, channel := range channels {
					c := models.Channel{}

					if err := utils.Merge(&c, channel); err != nil {
						log.Error("Error merging channel(%s): %s", channel.ID, err.Error())
						continue
					}

					c.Team = teamID

					if _, err = db.Channels.UpsertId(c.ID, &c); err != nil {
						log.Error("Error upserting channel(%s): %s", channel.ID, err.Error())
						continue
					}

					count += 1
				}

				log.Info("Syncing channels completed (%s): %#v %d", teamID, err, count)
			}
		}(ac.team.ID)

		for _, channel := range ac.channels {
			ac.ab.work <- func(teamID string, channelID string) func() {
				return func() {
					log.Info("Syncing latest channel messages: %s", channelID)

					params := slack.NewHistoryParameters()
					params.Inclusive = true

					var history *slack.History
					if v, err := ac.GetChannelHistory(channelID, params); err != nil {
						log.Error("Error retrieving channel history: %s", err.Error())
						return
					} else {
						history = v
					}

					for _, message := range history.Messages {
						m := models.Message{}

						if err := utils.Merge(&m, message.Msg); err != nil {
							log.Error("Error merging message: %s", err.Error())
							continue
						}

						m.ID = fmt.Sprintf("%s-%s-%s", teamID, channelID, m.Timestamp)
						m.Team = teamID
						m.Channel = channelID

						ac.ab.messageChan <- NewMessage("message", m)
					}

					log.Info("Syncing latest channels messages completed: %s", channelID)
				}
			}(ac.team.ID, channel.ID)
		}
	}

	return nil
}

type Message struct {
	Category string
	Body     interface{}
}

func NewMessage(category string, body interface{}) Message {
	return Message{
		Category: category,
		Body:     body,
	}
}

func (ac *archiveClient) Bot() {
	rtm := ac.NewRTM()
	go rtm.ManageConnection()

Loop:
	for {
		select {
		case msg := <-rtm.IncomingEvents:
			switch ev := msg.Data.(type) {
			case *slack.HelloEvent:
				// Ignore hello
			case *slack.ConnectedEvent:
				// log.Debug("Connection counter: %d %s", ev.ConnectionCount, ev.Info.Team.Domain)
				ac.channels = ev.Info.Channels
			case *slack.MessageEvent:
				m := models.Message{}

				if err := utils.Merge(&m, msg.Data.(*slack.MessageEvent).Msg); err != nil {
					log.Error("Merge error: %s", err.Error())
					continue
				}

				m.ID = fmt.Sprintf("%s-%s-%s", ac.team.ID, m.Channel, m.Timestamp)
				m.Team = ac.team.ID

				ac.ab.messageChan <- NewMessage("message", m)
			case *slack.PresenceChangeEvent:
			//	log.Debug("Presence Change: %v", ev)
			case *slack.LatencyReport:
			//	log.Debug("Current latency: %v", ev.Value)
			case *slack.UserTypingEvent:
				log.Debug("User(%s) typing...", msg.Data.(*slack.UserTypingEvent).User)
			case *slack.RTMError:
				log.Debug("Error: %s", ev.Error())
			case *slack.ConnectingEvent:
				log.Debug("Connecting...")
			case slack.RTMEvent:
				// 2016/09/04 12:19:18 Unexpected: slack.RTMEvent{Type:"connection_error", Data:(*slack.ConnectionErrorEvent)(0xc4243f6380
				if err, ok := ev.Data.(error); ok {
					log.Error("Event: %s %s", ev.Type, err.Error())
				} else {
					log.Debug("Event: %s", ev.Type)
				}
			case *slack.ReconnectUrlEvent:
				// log.Debug("ReconnectURL: %#v", ev)
			case *slack.InvalidAuthEvent:
				log.Debug("Invalid credentials")
				break Loop
			// case *slack.DesktopNotification:
			default:
				// Ignore other events..
				log.Debug("Unexpected: %s, %#v", ac.team.ID, ac.team.Domain, msg)
			}
		}
	}
}

func (ab *archiveBot) NewArchiveClient(token string) (*archiveClient, error) {
	ac := archiveClient{
		slack.New(token),
		ab,
		token,
		nil,
		nil,
	}

	ac.SetDebug(true)

	if team, err := ac.GetTeamInfo(); err != nil {
		return nil, err
	} else {
		ac.team = team

		ab.archivers[team.ID] = &ac
	}

	return &ac, nil
}

func (ac *archiveClient) Start() {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				trace := make([]byte, 1024)
				count := runtime.Stack(trace, true)
				log.Error("Error: %s", err)
				log.Debug("Stack of %d bytes: %s", count, trace)
				return
			}
		}()

		if err := ac.Sync(); err != nil {
			log.Error("Sync error: %s", err.Error())
		}

	}()

	go func() {
		defer func() {
			if err := recover(); err != nil {
				trace := make([]byte, 1024)
				count := runtime.Stack(trace, true)
				log.Error("Error: %s", err)
				log.Debug("Stack of %d bytes: %s", count, trace)
				return
			}
		}()

		ac.Bot()
	}()
}

func (ab *archiveBot) worker() {
	// this runs periodically with a random time, specific functions
	// to prevent rate limiting to happen

	for {
		fn := <-ab.work

		wait := 5 + rand.Intn(5)

		log.Info("Waiting for %d seconds", wait)

		time.Sleep(time.Duration(wait) * time.Second)

		fn()
	}
}

func (ab *archiveBot) Reload() {
}

// test feed
func (ab *archiveBot) test() {
	for {
		m := models.Message{}

		teamID := "TEST"
		channelID := "TEST"

		m.ID = fmt.Sprintf("%s-%s-%d", teamID, channelID, time.Now().Unix())
		m.Team = teamID
		m.Channel = channelID

		ab.messageChan <- NewMessage("message", m)

		fmt.Printf("Pushed message %+v\n", m)
		time.Sleep(5 * time.Second)
	}
}

func (ab *archiveBot) Start() {
	go ab.runMessageHandler2()
	go ab.worker()

	session := ab.session.Copy()
	defer session.Close()

	db := Database(session)

	tokens := ab.config.Tokens
	for _, token := range tokens {
		var team models.Team
		if err := db.Teams.Find(bson.M{
			"token": token,
		}).One(&team); err == nil {
			log.Info("Starting archive bot for team: %s", team.Domain)
		} else {
			log.Info("Starting archive bot for token: %s", token)
		}

		ac, err := ab.NewArchiveClient(token)
		if err == nil {
		} else if err.Error() == "invalid_auth" || err.Error() == "account_inactive" {
			continue
		} else if err != nil {
			log.Error("Error starting client %s: %s", token, err)
			continue
		}

		ac.Start()
	}
}
