package mongodb_manager

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"
)

type MongoMetadataStore struct {
	client *mongo.Client
	dbName string
}

type kvDocument struct {
	Key  string `bson:"_id"`
	Data []byte `bson:"data"`
}

func OpenDatabase(uri string) (*MongoMetadataStore, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cs, err := connstring.ParseAndValidate(uri)
	if err != nil {
		return nil, fmt.Errorf("failed to parse mongodb uri: %v", err)
	}

	dbName := cs.Database
	if dbName == "" {
		dbName = "chunkmesh_metadata"
	}

	clientOpts := options.Client().ApplyURI(uri)
	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to mongodb: %v", err)
	}

	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return nil, fmt.Errorf("failed to ping mongodb: %v", err)
	}

	return &MongoMetadataStore{
		client: client,
		dbName: dbName,
	}, nil
}

func (m *MongoMetadataStore) EnsureCollection(ctx context.Context, collection string) error {
	return nil
}

func (m *MongoMetadataStore) GetData(ctx context.Context, collection string, key string) ([]byte, error) {
	coll := m.client.Database(m.dbName).Collection(collection)

	var doc kvDocument
	err := coll.FindOne(ctx, bson.M{"_id": key}).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, fmt.Errorf("key '%s' not found in collection '%s'", key, collection)
		}
		return nil, err
	}

	return doc.Data, nil
}

func (m *MongoMetadataStore) PutData(ctx context.Context, collection string, key string, data []byte) error {
	coll := m.client.Database(m.dbName).Collection(collection)

	opts := options.Replace().SetUpsert(true)
	doc := kvDocument{
		Key:  key,
		Data: data,
	}

	_, err := coll.ReplaceOne(ctx, bson.M{"_id": key}, doc, opts)
	if err != nil {
		return fmt.Errorf("failed to put data in mongo: %v", err)
	}
	return nil
}

func (m *MongoMetadataStore) DeleteKey(ctx context.Context, collection string, key string) error {
	coll := m.client.Database(m.dbName).Collection(collection)

	_, err := coll.DeleteOne(ctx, bson.M{"_id": key})
	if err != nil {
		return fmt.Errorf("failed to delete key '%s': %v", key, err)
	}
	return nil
}

func (m *MongoMetadataStore) ExistsKey(ctx context.Context, collection string, key string) (bool, error) {
	coll := m.client.Database(m.dbName).Collection(collection)

	count, err := coll.CountDocuments(ctx, bson.M{"_id": key})
	if err != nil {
		return false, err
	}

	return count > 0, nil
}

func (m *MongoMetadataStore) GetAllData(ctx context.Context, collection string) (map[string][]byte, error) {
	coll := m.client.Database(m.dbName).Collection(collection)

	cursor, err := coll.Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	results := make(map[string][]byte)
	for cursor.Next(ctx) {
		var doc kvDocument
		if err := cursor.Decode(&doc); err != nil {
			return nil, err
		}
		results[doc.Key] = doc.Data
	}

	if err := cursor.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

func (m *MongoMetadataStore) GetAllKeys(ctx context.Context, collection string) ([]string, error) {
	coll := m.client.Database(m.dbName).Collection(collection)

	opts := options.Find().SetProjection(bson.M{"_id": 1})
	cursor, err := coll.Find(ctx, bson.M{}, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var keys []string
	for cursor.Next(ctx) {
		var doc kvDocument
		if err := cursor.Decode(&doc); err != nil {
			return nil, err
		}
		keys = append(keys, doc.Key)
	}

	return keys, nil
}

func (m *MongoMetadataStore) Close(ctx context.Context) error {
	return m.client.Disconnect(ctx)
}
