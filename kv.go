package ppkv

import (
	"encoding/json"
	"github.com/armon/consul-api" // TODO: Lock to branch
	"path"
)

type Client struct {
	kv *consulapi.KV
}

func NewClient() (*Client, error) {
	client, err := consulapi.NewClient(consulapi.DefaultConfig())

	if err != nil {
		return nil, err
	}

	return &Client{
		kv: client.KV(),
	}, nil
}

func (c *Client) List(query string) (map[string]interface{}, error) {
	xs, _, err := c.kv.List(query, nil)
	if err != nil {
		return nil, err
	}

	ret := map[string]interface{}{}

	for _, x := range xs {
		var value interface{}
		keyName := path.Base(x.Key)
		err := json.Unmarshal(x.Value, &value)
		if err != nil {
			return nil, err
		}
		ret[keyName] = value
	}
	return ret, nil
}

func (c *Client) Get(query string) (interface{}, error) {
	data, _, err := c.kv.Get(query, nil)
	if err == nil && data != nil {
		var ret interface{}
		jsonErr := json.Unmarshal(data.Value, &ret)
		if jsonErr != nil {
			return nil, err
		}
		return ret, nil
	}
	return nil, err
}

func (c *Client) DeleteTree(query string) error {
	_, err := c.kv.DeleteTree(query, nil)
	return err
}

func (c *Client) Put(key string, value interface{}) error {
	body, _ := json.Marshal(value)

	node := &consulapi.KVPair{
		Key:   key,
		Value: body,
	}
	_, err := c.kv.Put(node, nil)
	return err
}
