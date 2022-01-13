package ddbps

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
)

var tableName = "testtable"

func startDDBLocal() (func() error, error) {
	cmd := exec.Command("docker", "run", "-d", "-p", "8000:8000", "amazon/dynamodb-local")
	buf := &bytes.Buffer{}
	cmd.Stdout = buf
	cmd.Stderr = buf
	err := cmd.Run()
	if err != nil {
		return nil, fmt.Errorf("error running DynamoDB Local (%s), output:\n%s", err.Error(), buf)
	}
	ctrID := strings.TrimSpace(buf.String())

	return func() error {
		cmd := exec.Command("docker", "kill", ctrID)
		return cmd.Run()
	}, nil
}

func newDDBClient() *dynamodb.Client {
	resolver := dynamodb.EndpointResolverFunc(func(region string, options dynamodb.EndpointResolverOptions) (aws.Endpoint, error) {
		return aws.Endpoint{
			PartitionID:   "aws",
			URL:           "http://localhost:8000",
			SigningRegion: region,
		}, nil
	})
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("a", "a", "a")),
	)
	if err != nil {
		panic(err)
	}
	return dynamodb.NewFromConfig(cfg, dynamodb.WithEndpointResolver(resolver))
}

func setupTables(ddbClient *dynamodb.Client) error {
	_, err := ddbClient.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("key"),
				AttributeType: types.ScalarAttributeTypeB,
			},
			{
				AttributeName: aws.String("ttl"),
				AttributeType: types.ScalarAttributeTypeN,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("key"),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String("ttl"),
				KeyType:       types.KeyTypeRange,
			},
		},
		TableName:   &tableName,
		BillingMode: types.BillingModeProvisioned,
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1000),
			WriteCapacityUnits: aws.Int64(1000),
		},
	})
	return err
}

type mockPeerStore struct {
	addrs map[string][]multiaddr.Multiaddr
}

func (m *mockPeerStore) PeerInfo(peerID peer.ID) peer.AddrInfo {
	return peer.AddrInfo{
		ID:    peerID,
		Addrs: m.addrs[string(peerID)],
	}
}
func (m *mockPeerStore) AddAddrs(p peer.ID, addrs []multiaddr.Multiaddr, ttl time.Duration) {
	m.addrs[string(p)] = append(m.addrs[string(p)], addrs...)
}

func TestProviderStore(t *testing.T) {
	ctx := context.Background()

	// run dynamodb local first
	// docker run -p 8000:8000 amazon/dynamodb-local -jar DynamoDBLocal.jar -sharedDb
	stopDDBLocal, err := startDDBLocal()
	if err != nil {
		t.Fatal(err)
	}
	defer stopDDBLocal()

	ddb := newDDBClient()

	err = setupTables(ddb)
	if err != nil {
		t.Fatal(err)
	}

	peerStore := &mockPeerStore{addrs: map[string][]multiaddr.Multiaddr{}}
	provMgr := DynamoDBProviderStore{
		DDBClient:  ddb,
		PeerStore:  peerStore,
		TableName:  tableName,
		TTL:        10 * time.Second,
		QueryLimit: 100,
	}

	key := []byte("foo")
	ma, err := multiaddr.NewMultiaddr("/ip4/1.1.1.1")
	prov := peer.AddrInfo{
		ID:    peer.ID("peerid"),
		Addrs: []multiaddr.Multiaddr{ma},
	}
	err = provMgr.AddProvider(ctx, key, prov)
	if err != nil {
		t.Fatal(err)
	}

	provs, err := provMgr.GetProviders(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range provs {
		fmt.Printf("%s (%s)\n", p.String(), string(p.ID))
	}

}
