package mongo

import (
	"fmt"
	"time"

	"github.com/jollyburger/xmgo"

	mgo "gopkg.in/mgo.v2"
)

type MongoConfig struct {
	Mode           string        `json:"mode"`
	Addrs          []string      `json:"address"`
	UserName       string        `json:"username"`
	Passwd         string        `json:"passwd"`
	Database       string        `json:"database"`
	ReplicaSetName string        `json:"replica"`
	Timeout        time.Duration `json:"timeout"`
	PoolSize       int           `json:"pool_size"`
}

var (
	SINGLE_MODE  = "single"
	CLUSTER_MODE = "cluster"
)

var (
	analysisResultDbClient *MongoDbClient
)

func InitMongo(conf MongoConfig) {
	if conf.Mode != SINGLE_MODE && conf.Mode != CLUSTER_MODE {
		panic("mongodb cluster error")
	}

	if len(conf.Addrs) == 0 {
		panic("mongodb entrypoint error")
	}

	if conf.UserName == "" || conf.Passwd == "" {
		panic("mongodb authentication error")
	}

	analysisResultDbClient = NewMongoDbClient(conf)

	if analysisResultDbClient == nil {
		panic("init mongodb pool error")
	}
}

type MongoDbClient struct {
	dbConf MongoConfig
	pool   *xmgo.MgoPool
}

func NewMongoDbClient(conf MongoConfig) *MongoDbClient {
	client := MongoDbClient{
		dbConf: conf,
	}

	client.init()

	return &client
}

func (c *MongoDbClient) init() {
	conf := c.dbConf
	if conf.Mode == "single" {
		conf.Addrs[0] = fmt.Sprintf("mongodb://%s:%s@%s/%s", conf.UserName,
			conf.Passwd, conf.Addrs[0], conf.Database)
	}
	c.pool = xmgo.InitMgoPool(conf.Mode, conf.Addrs, conf.Timeout*time.Second,
		conf.UserName, conf.Passwd, conf.PoolSize)
}

func (c *MongoDbClient) GetSession() (*mgo.Session, error) {
	if c.pool == nil {
		panic("The db pool is not initialized")
	}
	return c.pool.Get()
}

func (c *MongoDbClient) PutSessionToPool(session *mgo.Session) {
	c.pool.Put(session)
}

func (c *MongoDbClient) CloseSessions() {
	c.pool.CloseAll()
}

func (c *MongoDbClient) GetDbName() string {
	return c.dbConf.Database
}

func GetAnalysisResultDbClient() *MongoDbClient {
	if analysisResultDbClient == nil {
		panic("analysis db client is not initialized yet")
	}
	return analysisResultDbClient
}
