package handlers

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/utils/io"
	"github.com/alpacahq/marketstore/utils/log"
	"github.com/buger/jsonparser"
	nats "github.com/nats-io/go-nats"
)

const ConditionExchangeSummary = 51

func Bar(msg *nats.Msg, backfillM *sync.Map) {
	// quickly parse the json
	symbol, _ := jsonparser.GetString(msg.Data, "sym")

	if strings.Contains(symbol, "/") {
		return
	}

	open, _ := jsonparser.GetFloat(msg.Data, "o")
	high, _ := jsonparser.GetFloat(msg.Data, "h")
	low, _ := jsonparser.GetFloat(msg.Data, "l")
	close, _ := jsonparser.GetFloat(msg.Data, "c")
	volume, _ := jsonparser.GetInt(msg.Data, "v")
	epochMillis, _ := jsonparser.GetInt(msg.Data, "s")

	epoch := epochMillis / 1000

	backfillM.LoadOrStore(symbol, &epoch)

	tbk := io.NewTimeBucketKeyFromString(fmt.Sprintf("%s/1Min/OHLCV", symbol))
	csm := io.NewColumnSeriesMap()

	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{epoch})
	cs.AddColumn("Open", []float32{float32(open)})
	cs.AddColumn("High", []float32{float32(high)})
	cs.AddColumn("Low", []float32{float32(low)})
	cs.AddColumn("Close", []float32{float32(close)})
	cs.AddColumn("Volume", []int32{int32(volume)})
	csm.AddColumnSeries(*tbk, cs)

	if err := executor.WriteCSM(csm, false); err != nil {
		log.Error("[polygon] csm write failure for key: [%v] (%v)", tbk.String(), err)
	}
}

func Trade(msg *nats.Msg) {
	var skip = false

	// get the condition in case we should ignore this quote
	jsonparser.ArrayEach(msg.Data, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
		if c, _ := strconv.Atoi(string(value)); c == ConditionExchangeSummary {
			skip = true
		}
	}, "c")

	if skip {
		return
	}

	// parse symbol and swap / for .
	symbol, _ := jsonparser.GetString(msg.Data, "sym")
	symbol = strings.Replace(symbol, "/", ".", 1)

	tbk := io.NewTimeBucketKey(symbol + "/1Min/TRADE")

	csm := io.NewColumnSeriesMap()
	cs := io.NewColumnSeries()

	size, _ := jsonparser.GetInt(msg.Data, "s")
	px, _ := jsonparser.GetFloat(msg.Data, "p")
	epochMillis, _ := jsonparser.GetInt(msg.Data, "t")

	timestamp := time.Unix(0, epochMillis*1000*1000)
	bucketTimestamp := timestamp.Truncate(time.Minute)

	cs.AddColumn("Epoch", []int64{bucketTimestamp.Unix()})
	cs.AddColumn("Nanoseconds", []int32{int32(timestamp.UnixNano() - bucketTimestamp.UnixNano())})
	cs.AddColumn("Price", []float32{float32(px)})
	cs.AddColumn("Size", []int32{int32(size)})
	csm.AddColumnSeries(*tbk, cs)

	if err := executor.WriteCSM(csm, true); err != nil {
		log.Error("[polygon] csm write failure for key: [%v] (%v)", tbk.String(), err)
	}
}

func Quote(msg *nats.Msg) {
	// parse symbol and swap / for .
	symbol, _ := jsonparser.GetString(msg.Data, "sym")
	symbol = strings.Replace(symbol, "/", ".", 1)

	tbk := io.NewTimeBucketKey(symbol + "/1Min/QUOTE")

	csm := io.NewColumnSeriesMap()
	cs := io.NewColumnSeries()

	// parse the quote fields
	bidPx, _ := jsonparser.GetFloat(msg.Data, "bp")
	askPx, _ := jsonparser.GetFloat(msg.Data, "ap")
	bidSize, _ := jsonparser.GetInt(msg.Data, "bs")
	askSize, _ := jsonparser.GetInt(msg.Data, "as")
	epochMillis, _ := jsonparser.GetInt(msg.Data, "t")

	timestamp := time.Unix(0, epochMillis*1000*1000)
	bucketTimestamp := timestamp.Truncate(time.Minute)

	cs.AddColumn("Epoch", []int64{bucketTimestamp.Unix()})
	cs.AddColumn("Nanoseconds", []int32{int32(timestamp.UnixNano() - bucketTimestamp.UnixNano())})
	cs.AddColumn("BidPrice", []float32{float32(bidPx)})
	cs.AddColumn("AskPrice", []float32{float32(askPx)})
	cs.AddColumn("BidSize", []int32{int32(bidSize)})
	cs.AddColumn("AskSize", []int32{int32(askSize)})
	csm.AddColumnSeries(*tbk, cs)

	if err := executor.WriteCSM(csm, true); err != nil {
		log.Error("[polygon] csm write failure for key: [%v] (%v)", tbk.String(), err)
	}
}
