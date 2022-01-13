package ddbps

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/multiformats/go-multiaddr"
)

type peerStore interface {
	PeerInfo(peer.ID) peer.AddrInfo
	AddAddrs(p peer.ID, addrs []multiaddr.Multiaddr, ttl time.Duration)
}

type DynamoDBProviderStore struct {
	Self       peer.ID
	PeerStore  peerStore
	DDBClient  *dynamodb.Client
	TableName  string
	TTL        time.Duration
	QueryLimit int32
}

// the range key is NOT the provider, it is the TTL, so that we get items back in TTL-order
// b/c we want to prefer newer providers and, if we need to, we drop the older ones

func (d *DynamoDBProviderStore) AddProvider(ctx context.Context, key []byte, prov peer.AddrInfo) error {
	fmt.Printf("AddProvider\n")
	if prov.ID != d.Self { // don't add own addrs.
		d.PeerStore.AddAddrs(prov.ID, prov.Addrs, peerstore.ProviderAddrTTL)
	}

	ttlEpoch := time.Now().Add(d.TTL).Unix()
	ttlEpochStr := strconv.FormatInt(ttlEpoch, 10)
	_, err := d.DDBClient.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &d.TableName,
		Item: map[string]types.AttributeValue{
			"key":  &types.AttributeValueMemberB{Value: key},
			"prov": &types.AttributeValueMemberB{Value: []byte(prov.ID)},
			"ttl":  &types.AttributeValueMemberN{Value: ttlEpochStr},
		},
	})
	if err != nil {
		fmt.Printf("add provider error: %s\n", err)
	}
	return err
}

func (d *DynamoDBProviderStore) GetProviders(ctx context.Context, key []byte) ([]peer.AddrInfo, error) {
	fmt.Printf("GetProviders\n")

	paginator := dynamodb.NewQueryPaginator(d.DDBClient, &dynamodb.QueryInput{
		TableName: &d.TableName,
		//		KeyConditionExpression: aws.String("#k = :key and #t > :threshold"),
		KeyConditionExpression: aws.String("#k = :key"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":key": &types.AttributeValueMemberB{Value: key},
		},
		ExpressionAttributeNames: map[string]string{
			"#k": "key",
		},
		ScanIndexForward: aws.Bool(false), // return most recent entries first
		Limit:            &d.QueryLimit,
	})
	providers := []peer.AddrInfo{}
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, item := range page.Items {
			prov, ok := item["prov"]
			if !ok {
				return nil, errors.New("unexpected item without a 'prov' attribute")
			}
			provB, ok := prov.(*types.AttributeValueMemberB)
			if !ok {

				return nil, fmt.Errorf("unexpected value type of '%s' for 'prov' attribute", reflect.TypeOf(prov))
			}
			peerID := peer.ID(string(provB.Value))
			addrInfo := d.PeerStore.PeerInfo(peerID)
			providers = append(providers, addrInfo)
			fmt.Printf("Found provider: %s\n", addrInfo.String())
		}
	}
	fmt.Printf("found %d providers\n", len(providers))
	return providers, nil
}
