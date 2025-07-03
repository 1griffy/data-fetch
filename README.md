# Binance Data Fetcher

A Go application that fetches minute-by-minute cryptocurrency data from Binance using both REST API and WebSocket connections, with multi-threading support and funding rate integration.

## Features

- **Historical Data**: Fetches historical kline/candlestick data using Binance REST API
- **Real-time Data**: Streams live minute-by-minute data using Binance WebSocket
- **Multi-threading**: Each symbol runs in its own goroutine for parallel processing
- **Rate Limiting**: Built-in rate limiting to respect Binance's API limits (1200 requests/minute)
- **Funding Rates**: Includes funding rate data corresponding to each minute
- **Multiple Symbols**: Supports multiple cryptocurrency pairs (BTCUSDT, ETHUSDT, BNBUSDT, etc.)
- **CSV Export**: Saves all data to CSV files with comprehensive information
- **Graceful Shutdown**: Handles shutdown signals properly
- **Structured Logging**: Uses logrus for comprehensive logging
- **Error Handling**: Robust error handling and reconnection logic

## Prerequisites

- Go 1.21 or higher
- Internet connection to access Binance APIs

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd binance-data-fetcher
```

2. Install dependencies:
```bash
go mod tidy
```

3. (Optional) Create a `.env` file for configuration:
```bash
cp env.example .env
```

## Usage

### Basic Usage

Run the application:
```bash
go run main.go
```

The application will:
1. Fetch the last 60 days of historical 1-minute klines for configured symbols
2. Fetch corresponding funding rate data for each symbol
3. Process data in parallel using multiple goroutines
4. Save all data to CSV files in the `data/` directory
5. Connect to Binance WebSocket for real-time data
6. Continue running until you press Ctrl+C

### Configuration

You can modify the symbols in `main.go`:

```go
symbols := []string{"BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "DOTUSDT"}
```

### Environment Variables

Create a `.env` file for custom configuration:

```env
# Log level (debug, info, warn, error)
LOG_LEVEL=info

# Number of historical klines to fetch
HISTORICAL_LIMIT=100

# WebSocket buffer size
WS_BUFFER_SIZE=1000

# Binance API base URL (usually don't need to change)
BINANCE_API_URL=https://api.binance.com

# Binance WebSocket URL (usually don't need to change)
BINANCE_WS_URL=wss://stream.binance.com:9443/ws

# Symbols to track (comma-separated)
SYMBOLS=BTCUSDT,ETHUSDT,BNBUSDT

# Kline interval (1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M)
KLINE_INTERVAL=1m
```

## Data Structure

The application processes comprehensive data structures:

### KlineData
- `OpenTime`: Opening time of the kline
- `OpenPrice`: Opening price
- `HighPrice`: Highest price during the period
- `LowPrice`: Lowest price during the period
- `ClosePrice`: Closing price
- `Volume`: Trading volume
- `CloseTime`: Closing time of the kline
- `QuoteAssetVolume`: Quote asset volume
- `NumberOfTrades`: Number of trades
- `TakerBuyBase`: Taker buy base asset volume
- `TakerBuyQuote`: Taker buy quote asset volume

### FundingRateData
- `Symbol`: Trading pair symbol
- `FundingRate`: Current funding rate
- `FundingTime`: Last funding time

## CSV Output

The application creates CSV files with the following columns:

1. **Symbol** - Trading pair
2. **OpenTime** - Opening timestamp
3. **OpenTimeFormatted** - Human-readable opening time
4. **OpenPrice** - Opening price
5. **HighPrice** - Highest price
6. **LowPrice** - Lowest price
7. **ClosePrice** - Closing price
8. **Volume** - Trading volume
9. **CloseTime** - Closing timestamp
10. **CloseTimeFormatted** - Human-readable closing time
11. **QuoteAssetVolume** - Quote asset volume
12. **NumberOfTrades** - Number of trades
13. **TakerBuyBase** - Taker buy base volume
14. **TakerBuyQuote** - Taker buy quote volume
15. **FundingRate** - Funding rate at that time
16. **FundingTime** - Funding timestamp
17. **FundingTimeFormatted** - Human-readable funding time
18. **MarkPrice** - Mark price
19. **IndexPrice** - Index price
20. **EstimatedRate** - Estimated settlement price
21. **NextFundingTime** - Next funding timestamp
22. **NextFundingTimeFormatted** - Human-readable next funding time
23. **InterestRate** - Interest rate

## Architecture

The application consists of several main components:

1. **BinanceClient**: Handles REST API calls for historical data and funding rates
2. **WebSocketClient**: Manages WebSocket connections for real-time data
3. **DataProcessor**: Processes and handles incoming kline data with thread-safe CSV writing
4. **Rate Limiter**: Ensures API calls respect Binance's rate limits
5. **Multi-threading**: Each symbol runs in its own goroutine for parallel processing

## Multi-threading Implementation

- **Parallel Processing**: Each symbol is processed in its own goroutine
- **Rate Limiting**: Each client has its own rate limiter (1000 requests/minute)
- **Thread Safety**: CSV writing is protected with read-write mutexes
- **Error Handling**: Errors from goroutines are collected and reported
- **Resource Management**: Proper cleanup of resources and graceful shutdown

## Rate Limits

The application respects Binance's API rate limits:
- **REST API**: 1200 requests per minute per IP
- **WebSocket**: No rate limits for public streams
- **Funding Rate API**: Included in the overall rate limit
- **Conservative Approach**: Uses 1000 requests/minute per client to stay well within limits

## Customization

### Adding Data Storage

To store data in a database, modify the `ProcessKlineData` method in `DataProcessor`:

```go
func (d *DataProcessor) ProcessKlineData(symbol string, data KlineData, fundingData *FundingRateData) {
    // Your custom processing logic here
    // Example: Store to PostgreSQL, MongoDB, etc.
    
    // Calculate technical indicators
    // Send alerts
    // Store to file
}
```

### Adding Technical Indicators

You can extend the `DataProcessor` to calculate technical indicators:

```go
func (d *DataProcessor) CalculateSMA(prices []float64, period int) float64 {
    if len(prices) < period {
        return 0
    }
    
    sum := 0.0
    for i := len(prices) - period; i < len(prices); i++ {
        sum += prices[i]
    }
    return sum / float64(period)
}
```

### Adding Alerts

Implement price and funding rate alerts:

```go
func (d *DataProcessor) ProcessKlineData(symbol string, data KlineData, fundingData *FundingRateData) {
    // Check for price alerts
    if data.ClosePrice > 50000 { // BTC price alert
        d.logger.Warn("BTC price above $50,000!")
    }
    
    // Check for funding rate alerts
    if fundingData != nil && fundingData.FundingRate > 0.01 { // 1% funding rate
        d.logger.Warn("High funding rate detected!")
    }
}
```

## Error Handling

The application includes comprehensive error handling:

- Network timeouts and retries
- WebSocket reconnection logic
- Rate limit enforcement
- Graceful shutdown on interrupt signals
- Data validation and sanitization
- Thread-safe error collection

## Performance

- **Multi-threaded**: Processes multiple symbols in parallel
- **Rate Limited**: Prevents API rate limit violations
- **Memory Efficient**: Processes data in chunks
- **Fast CSV Writing**: Buffered CSV writing with periodic flushing

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Disclaimer

This software is for educational and research purposes. Use at your own risk. The authors are not responsible for any financial losses incurred through the use of this software.

## Support

For issues and questions:
1. Check the existing issues
2. Create a new issue with detailed information
3. Include logs and error messages # data-fetch
