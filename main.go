package main

import (
	"context"
	"fmt"
	"log"
	"time"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"github.com/gookit/config/v2"
)

type Bar struct {
	Date time.Time `bson:"date"`
	Close float64 `bson:"close"`
	Open float64 `bson:"open"`
	High float64 `bson:"high"`
	Low float64 `bson:"low"`
	Volume int `bson:"volume"`
}

type Historical struct {
	Data []Bar `bson:"data"`
}

type Stock struct {
	Group string `bson:"group"` 
	Isin string `bson:"isin"` 
	Name string `bson:"name"`
	//FullName string `bson:"fullName"`
	//Quotes []bson.D `bson:"quotes"`
	//YahooSearch bson.D `bson:"yahooSearch"`
	//YahooQuote bson.D `bson:"yahooQuote"`
	//StoxxQuote bson.D `bson:"stoxxQuote"`
	Historical Historical `bson:"historical"`
	RegionForMansfield string `bson:"regionForMansfield"`
	MansfieldTickerEsBolsa string `bson:"mansfieldTickerEsBolsa"`
	Error bool `bson:"error"`
}


func main() {
	config.WithOptions(config.ParseEnv)

	err := config.LoadFiles("config-dev.json")
	if err != nil {
		panic(err)
	}
	PASSWORD := config.String("PASSWORD")

	fmt.Println(time.Now())

	/*
		Connect to my cluster
	*/
	//localhost
	//uri := "mongodb://localhost:27017/?retryWrites=true&w=majority"
	//clientOptions := options.Client().
	//	ApplyURI(uri)


	//atlas
	uri := "mongodb+srv://josuamanuel:" + PASSWORD + "@cluster0.afmko.mongodb.net/?retryWrites=true&w=majority"
	serverAPIOptions := options.ServerAPI(options.ServerAPIVersion1)
	clientOptions := options.Client().
			ApplyURI(uri).
			SetServerAPIOptions(serverAPIOptions)

			
	// Same for localhost and atlas
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
			log.Fatal(err)
	}

	fmt.Println(time.Now())

	channel := make(chan Stock)

	var wg sync.WaitGroup
	wg.Add(1)
	go produceStock(ctx, channel, &wg, client)
	wg.Add(1)
	go consumeStock(ctx, channel, &wg)

	wg.Wait()
	fmt.Println(time.Now())
	fmt.Println("Ending...")
}

func produceStock (ctx context.Context, channel chan Stock, wg *sync.WaitGroup, client *mongo.Client) {
	defer wg.Done()

	query := bson.D{
		{
			Key: "$or", Value: bson.A{
				bson.D{
					{Key: "group", Value: bson.D{{Key: "$exists", Value: true}}},
					{Key: "regionForMansfield", Value: "USA"},
					{Key: "yahooQuote.marketCap", Value: bson.D{{Key: "$gt", Value: 1000000000}}},
				},
				bson.D{
					{Key: "group", Value: bson.D{{Key: "$exists", Value: true}}},
					{Key: "regionForMansfield", Value: "EUROPE"},
					{Key: "yahooQuote.marketCap", Value: bson.D{{Key: "$gt", Value: 500000000}}},
				},
			},
		},
	}
	/*
		Get my collection instance
	*/
	collection := client.Database("stockMarket").Collection("stocks")

	cur, currErr := collection.Find(ctx, query)
	if currErr != nil {
		panic(currErr)
	}
	defer cur.Close(ctx)

	var result Stock

	for cur.Next(ctx) {
		if err := cur.Decode(&result); err != nil {
			log.Fatal(err)
		}
		channel <- result
	}

	close(channel)
}

func consumeStock (ctx context.Context, channel chan Stock, wg *sync.WaitGroup) {
	defer wg.Done()
	var stocksFound int

	var high, low, close float64
	var calc float64
	for stock:= range channel {

		stocksFound++
		for i := float64(0); i < 500 ; i++ { 
			for _, val := range stock.Historical.Data {
				high, low, close = val.High, val.Low, val.Close
				if (val.High < 1 ) {
					high = 8
				}

				if (val.Close < 1 ) {
					close = 1
				}

				if (val.Low < 1 ) {
					low = 1
				}

				//calc += (close*low + i)/(close*high*close*high + i)
				calc += (close*low + low*high - i)/(low*(high + close + i))
			}
		}
	}

	fmt.Println(calc)
	fmt.Println(stocksFound)
	fmt.Println(time.Now())
}
