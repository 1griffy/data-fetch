package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

// KlineData represents a single kline/candlestick data point
type KlineData struct {
	OpenTime         int64   `json:"openTime"`
	OpenPrice        float64 `json:"openPrice"`
	HighPrice        float64 `json:"highPrice"`
	LowPrice         float64 `json:"lowPrice"`
	ClosePrice       float64 `json:"closePrice"`
	Volume           float64 `json:"volume"`
	CloseTime        int64   `json:"closeTime"`
	QuoteAssetVolume float64 `json:"quoteAssetVolume"`
	NumberOfTrades   int     `json:"numberOfTrades"`
	TakerBuyBase     float64 `json:"takerBuyBase"`
	TakerBuyQuote    float64 `json:"takerBuyQuote"`
}

// FundingRateData represents funding rate information
type FundingRateData struct {
	Symbol      string  `json:"symbol"`
	FundingRate float64 `json:"fundingRate"`
	FundingTime int64   `json:"fundingTime"`
}

// CombinedData represents kline data with corresponding funding rate
type CombinedData struct {
	KlineData       KlineData
	FundingRateData *FundingRateData
}

// RateLimitInfo tracks rate limit information
type RateLimitInfo struct {
	RemainingRequests int
	ResetTime         time.Time
	WeightUsed        int
	WeightLimit       int
}

// BinanceClient handles API calls to Binance
type BinanceClient struct {
	baseURL       string
	httpClient    *http.Client
	logger        *logrus.Logger
	rateLimiter   *rate.Limiter
	rateLimitInfo *RateLimitInfo
	mutex         sync.RWMutex
	fundingCache  map[string]map[int64]*FundingRateData // symbol -> fundingTime -> fundingRate
	cacheMutex    sync.RWMutex
}

// NewBinanceClient creates a new Binance client
func NewBinanceClient() *BinanceClient {
	return &BinanceClient{
		baseURL: "https://fapi.binance.com",
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		logger: logrus.New(),
		// Start with conservative rate limiting
		rateLimiter: rate.NewLimiter(rate.Every(time.Minute/800), 1), // 800 requests per minute
		rateLimitInfo: &RateLimitInfo{
			RemainingRequests: 1200,
			ResetTime:         time.Now().Add(time.Minute),
			WeightUsed:        0,
			WeightLimit:       1200,
		},
		mutex:        sync.RWMutex{},
		fundingCache: make(map[string]map[int64]*FundingRateData),
		cacheMutex:   sync.RWMutex{},
	}
}

// adjustRateLimit adjusts the rate limiter based on rate limit headers
func (c *BinanceClient) adjustRateLimit(resp *http.Response) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Parse rate limit headers
	if remaining := resp.Header.Get("X-MBX-USED-WEIGHT-1M"); remaining != "" {
		if used, err := strconv.Atoi(remaining); err == nil {
			c.rateLimitInfo.WeightUsed = used
			c.rateLimitInfo.WeightLimit = 1200 // Binance weight limit

			// Calculate remaining requests
			remainingRequests := c.rateLimitInfo.WeightLimit - c.rateLimitInfo.WeightUsed
			c.rateLimitInfo.RemainingRequests = remainingRequests

			// If we're using more than 80% of the limit, slow down
			if used > 960 { // 80% of 1200
				newRate := rate.Every(time.Minute / 600) // Slow down to 600 requests per minute
				c.rateLimiter.SetLimit(newRate)
				c.logger.Warnf("Rate limit approaching (used: %d/1200), slowing down to 600 requests/minute", used)
			} else if used > 720 { // 60% of 1200
				newRate := rate.Every(time.Minute / 700) // Slow down to 700 requests per minute
				c.rateLimiter.SetLimit(newRate)
				c.logger.Infof("Rate limit moderate (used: %d/1200), adjusting to 700 requests/minute", used)
			} else {
				// Reset to normal rate if we're well under the limit
				newRate := rate.Every(time.Minute / 800)
				c.rateLimiter.SetLimit(newRate)
			}
		}
	}

	// Parse reset time
	if resetTime := resp.Header.Get("X-MBX-USED-WEIGHT-1M-RESET"); resetTime != "" {
		if resetUnix, err := strconv.ParseInt(resetTime, 10, 64); err == nil {
			c.rateLimitInfo.ResetTime = time.Unix(resetUnix, 0)
		}
	}
}

// makeRequestWithRetry makes an HTTP request with retry logic and rate limit handling
func (c *BinanceClient) makeRequestWithRetry(method, url string) (*http.Response, error) {
	maxRetries := 5
	baseDelay := time.Second

	for attempt := 0; attempt < maxRetries; attempt++ {
		// Wait for rate limiter
		if err := c.rateLimiter.Wait(context.Background()); err != nil {
			return nil, fmt.Errorf("rate limiter error: %w", err)
		}

		// Make the request
		req, err := http.NewRequest(method, url, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			c.logger.Warnf("Request failed (attempt %d/%d): %v", attempt+1, maxRetries, err)
			if attempt == maxRetries-1 {
				return nil, fmt.Errorf("request failed after %d attempts: %w", maxRetries, err)
			}

			// Exponential backoff
			delay := time.Duration(attempt+1) * baseDelay
			c.logger.Infof("Retrying in %v...", delay)
			time.Sleep(delay)
			continue
		}

		// Handle rate limiting
		if resp.StatusCode == 429 {
			c.logger.Warnf("Rate limit hit (attempt %d/%d)", attempt+1, maxRetries)

			// Parse retry-after header
			retryAfter := resp.Header.Get("Retry-After")
			var delay time.Duration
			if retryAfter != "" {
				if seconds, err := strconv.Atoi(retryAfter); err == nil {
					delay = time.Duration(seconds) * time.Second
				} else {
					delay = time.Duration(attempt+1) * 30 * time.Second // Default 30s * attempt
				}
			} else {
				delay = time.Duration(attempt+1) * 30 * time.Second
			}

			c.logger.Infof("Waiting %v before retry...", delay)
			time.Sleep(delay)

			// Dramatically slow down the rate limiter
			c.mutex.Lock()
			c.rateLimiter.SetLimit(rate.Every(time.Minute / 200)) // 200 requests per minute
			c.mutex.Unlock()

			resp.Body.Close()
			continue
		}

		// Handle other HTTP errors
		if resp.StatusCode >= 400 && resp.StatusCode != 429 {
			resp.Body.Close()
			if attempt == maxRetries-1 {
				return nil, fmt.Errorf("HTTP request failed with status: %d", resp.StatusCode)
			}

			// Exponential backoff for other errors
			delay := time.Duration(attempt+1) * baseDelay
			c.logger.Infof("HTTP error on %s %d, retrying in %v...", url, resp.StatusCode, delay)
			time.Sleep(delay)
			continue
		}

		// Success - adjust rate limits based on headers
		c.adjustRateLimit(resp)
		return resp, nil
	}

	return nil, fmt.Errorf("request failed after %d attempts", maxRetries)
}

// GetKlineData fetches historical kline data for a symbol
func (c *BinanceClient) GetKlineData(symbol, interval string, limit int, startTime, endTime *int64) ([]KlineData, error) {
	url := fmt.Sprintf("%s/fapi/v1/klines?symbol=%s&interval=%s&limit=%d",
		c.baseURL, strings.ToUpper(symbol), interval, limit)

	// Add start time if provided
	if startTime != nil {
		url += fmt.Sprintf("&startTime=%d", *startTime)
	}

	// Add end time if provided
	if endTime != nil {
		url += fmt.Sprintf("&endTime=%d", *endTime)
	}

	resp, err := c.makeRequestWithRetry("GET", url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch kline data: %w", err)
	}
	defer resp.Body.Close()

	var rawData [][]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&rawData); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	var klines []KlineData
	for _, data := range rawData {
		if len(data) < 11 {
			continue
		}

		openTime, _ := strconv.ParseInt(fmt.Sprintf("%.0f", data[0]), 10, 64)
		openPrice, _ := strconv.ParseFloat(data[1].(string), 64)
		highPrice, _ := strconv.ParseFloat(data[2].(string), 64)
		lowPrice, _ := strconv.ParseFloat(data[3].(string), 64)
		closePrice, _ := strconv.ParseFloat(data[4].(string), 64)
		volume, _ := strconv.ParseFloat(data[5].(string), 64)
		closeTime, _ := strconv.ParseInt(fmt.Sprintf("%.0f", data[6]), 10, 64)
		quoteAssetVolume, _ := strconv.ParseFloat(data[7].(string), 64)
		numberOfTrades, _ := strconv.Atoi(fmt.Sprintf("%.0f", data[8]))
		takerBuyBase, _ := strconv.ParseFloat(data[9].(string), 64)
		takerBuyQuote, _ := strconv.ParseFloat(data[10].(string), 64)

		klines = append(klines, KlineData{
			OpenTime:         openTime,
			OpenPrice:        openPrice,
			HighPrice:        highPrice,
			LowPrice:         lowPrice,
			ClosePrice:       closePrice,
			Volume:           volume,
			CloseTime:        closeTime,
			QuoteAssetVolume: quoteAssetVolume,
			NumberOfTrades:   numberOfTrades,
			TakerBuyBase:     takerBuyBase,
			TakerBuyQuote:    takerBuyQuote,
		})
	}

	return klines, nil
}

// GetPremiumIndex fetches current premium index data for a symbol
func (c *BinanceClient) GetPremiumIndex(symbol string) (*FundingRateData, error) {
	url := fmt.Sprintf("%s/fapi/v1/premiumIndex?symbol=%s", c.baseURL, strings.ToUpper(symbol))

	resp, err := c.makeRequestWithRetry("GET", url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch premium index: %w", err)
	}
	defer resp.Body.Close()

	var rawData map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&rawData); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	// Safe parsing with nil checks
	fundingRate := 0.0
	if rawData["lastFundingRate"] != nil {
		if str, ok := rawData["lastFundingRate"].(string); ok {
			fundingRate, _ = strconv.ParseFloat(str, 64)
		}
	}

	fundingTime := int64(0)
	if rawData["lastFundingTime"] != nil {
		if num, ok := rawData["lastFundingTime"].(float64); ok {
			fundingTime = int64(num)
		}
	}

	return &FundingRateData{
		Symbol:      symbol,
		FundingRate: fundingRate,
		FundingTime: fundingTime,
	}, nil
}

// GetFundingRateForTime returns the funding rate that was in effect at the given time
func (c *BinanceClient) GetFundingRateForTime(symbol string, targetTime int64) (*FundingRateData, error) {
	// First, let's fetch a range of funding rates to understand the funding interval
	// We'll fetch funding rates from 24 hours before to 24 hours after the target time
	startTime := targetTime - (24 * 60 * 60 * 1000) // 24 hours back
	endTime := targetTime + (24 * 60 * 60 * 1000)   // 24 hours forward

	// Check cache first for any funding rates in this range
	c.cacheMutex.RLock()
	var cachedRates []*FundingRateData
	if symbolCache, exists := c.fundingCache[symbol]; exists {
		for fundingTime, rate := range symbolCache {
			if fundingTime >= startTime && fundingTime <= endTime {
				cachedRates = append(cachedRates, rate)
			}
		}
	}
	c.cacheMutex.RUnlock()

	// If we have enough cached data, use it
	if len(cachedRates) >= 3 {
		// Find the funding rate that was in effect at the target time
		var effectiveFundingRate *FundingRateData
		for _, rate := range cachedRates {
			if rate.FundingTime <= targetTime {
				if effectiveFundingRate == nil || rate.FundingTime > effectiveFundingRate.FundingTime {
					effectiveFundingRate = rate
				}
			}
		}
		if effectiveFundingRate != nil {
			return effectiveFundingRate, nil
		}
	}

	// Fetch funding rates from API
	url := fmt.Sprintf("%s/fapi/v1/fundingRate?symbol=%s&startTime=%d&endTime=%d&limit=1000",
		c.baseURL, strings.ToUpper(symbol), startTime, endTime)

	resp, err := c.makeRequestWithRetry("GET", url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch funding rates for time: %w", err)
	}
	defer resp.Body.Close()

	var rawData []map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&rawData); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	// Parse and cache all funding rates
	var fundingRates []*FundingRateData
	for _, data := range rawData {
		fundingTime := int64(0)
		if data["fundingTime"] != nil {
			if num, ok := data["fundingTime"].(float64); ok {
				fundingTime = int64(num)
			}
		}

		fundingRate := 0.0
		if data["fundingRate"] != nil {
			if str, ok := data["fundingRate"].(string); ok {
				fundingRate, _ = strconv.ParseFloat(str, 64)
			}
		}

		fundingRateData := &FundingRateData{
			Symbol:      symbol,
			FundingRate: fundingRate,
			FundingTime: fundingTime,
		}

		fundingRates = append(fundingRates, fundingRateData)

		// Cache this funding rate
		c.cacheMutex.Lock()
		if c.fundingCache[symbol] == nil {
			c.fundingCache[symbol] = make(map[int64]*FundingRateData)
		}
		c.fundingCache[symbol][fundingTime] = fundingRateData
		c.cacheMutex.Unlock()
	}

	// Find the funding rate that was in effect at the target time
	// Funding rates are effective from their funding time until the next funding time
	var effectiveFundingRate *FundingRateData
	for _, rate := range fundingRates {
		if rate.FundingTime <= targetTime {
			if effectiveFundingRate == nil || rate.FundingTime > effectiveFundingRate.FundingTime {
				effectiveFundingRate = rate
			}
		}
	}

	return effectiveFundingRate, nil
}

// DataProcessor handles the processing and storage of kline data
type DataProcessor struct {
	logger     *logrus.Logger
	csvWriters map[string]*csv.Writer
	csvFiles   map[string]*os.File
	mutex      sync.RWMutex
}

// NewDataProcessor creates a new data processor
func NewDataProcessor() *DataProcessor {
	return &DataProcessor{
		logger:     logrus.New(),
		csvWriters: make(map[string]*csv.Writer),
		csvFiles:   make(map[string]*os.File),
		mutex:      sync.RWMutex{},
	}
}

// InitializeCSVWriter creates a CSV file for a symbol
func (d *DataProcessor) InitializeCSVWriter(symbol string) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	// Create data directory if it doesn't exist
	if err := os.MkdirAll("data", 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	// Create CSV file
	filename := fmt.Sprintf("data/%s_60days_futures_with_funding.csv", strings.ToLower(symbol))
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create CSV file for %s: %w", symbol, err)
	}

	writer := csv.NewWriter(file)

	// Write CSV header with funding rate columns
	header := []string{
		"Symbol",
		"OpenTime",
		"OpenTimeFormatted",
		"OpenPrice",
		"HighPrice",
		"LowPrice",
		"ClosePrice",
		"Volume",
		"CloseTime",
		"CloseTimeFormatted",
		"QuoteAssetVolume",
		"NumberOfTrades",
		"TakerBuyBase",
		"TakerBuyQuote",
		"FundingRate",
		"FundingTime",
		"FundingTimeFormatted",
	}

	if err := writer.Write(header); err != nil {
		file.Close()
		return fmt.Errorf("failed to write CSV header for %s: %w", symbol, err)
	}

	d.csvWriters[symbol] = writer
	d.csvFiles[symbol] = file

	d.logger.Infof("Created CSV file: %s", filename)
	return nil
}

// WriteKlineToCSV writes a kline data point to the appropriate CSV file
func (d *DataProcessor) WriteKlineToCSV(symbol string, data KlineData, fundingData *FundingRateData) error {
	d.mutex.RLock()
	writer, exists := d.csvWriters[symbol]
	d.mutex.RUnlock()

	if !exists {
		return fmt.Errorf("CSV writer not initialized for symbol: %s", symbol)
	}

	// Convert timestamps to readable format
	openTimeFormatted := time.Unix(data.OpenTime/1000, 0).Format("2006-01-02 15:04:05")
	closeTimeFormatted := time.Unix(data.CloseTime/1000, 0).Format("2006-01-02 15:04:05")

	// Prepare CSV row
	row := []string{
		symbol,
		strconv.FormatInt(data.OpenTime, 10),
		openTimeFormatted,
		strconv.FormatFloat(data.OpenPrice, 'f', -1, 64),
		strconv.FormatFloat(data.HighPrice, 'f', -1, 64),
		strconv.FormatFloat(data.LowPrice, 'f', -1, 64),
		strconv.FormatFloat(data.ClosePrice, 'f', -1, 64),
		strconv.FormatFloat(data.Volume, 'f', -1, 64),
		strconv.FormatInt(data.CloseTime, 10),
		closeTimeFormatted,
		strconv.FormatFloat(data.QuoteAssetVolume, 'f', -1, 64),
		strconv.Itoa(data.NumberOfTrades),
		strconv.FormatFloat(data.TakerBuyBase, 'f', -1, 64),
		strconv.FormatFloat(data.TakerBuyQuote, 'f', -1, 64),
	}

	// Add funding rate data if available
	if fundingData != nil {
		fundingTimeFormatted := time.Unix(fundingData.FundingTime/1000, 0).Format("2006-01-02 15:04:05")

		row = append(row,
			strconv.FormatFloat(fundingData.FundingRate, 'f', -1, 64),
			strconv.FormatInt(fundingData.FundingTime, 10),
			fundingTimeFormatted,
		)
	} else {
		// Add empty values for funding rate columns
		row = append(row, "", "", "")
	}

	if err := writer.Write(row); err != nil {
		return fmt.Errorf("failed to write CSV row for %s: %w", symbol, err)
	}

	writer.Flush()
	return nil
}

// CloseCSVFiles closes all CSV files
func (d *DataProcessor) CloseCSVFiles() {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	for symbol, writer := range d.csvWriters {
		writer.Flush()
		if file, exists := d.csvFiles[symbol]; exists {
			file.Close()
			d.logger.Infof("Closed CSV file for %s", symbol)
		}
	}
}

// ProcessKlineData processes incoming kline data
func (d *DataProcessor) ProcessKlineData(symbol string, data KlineData, fundingData *FundingRateData) {
	// Convert timestamp to readable format
	openTime := time.Unix(data.OpenTime/1000, 0)
	closeTime := time.Unix(data.CloseTime/1000, 0)

	logFields := logrus.Fields{
		"symbol":     symbol,
		"openTime":   openTime.Format("2006-01-02 15:04:05"),
		"closeTime":  closeTime.Format("2006-01-02 15:04:05"),
		"openPrice":  data.OpenPrice,
		"highPrice":  data.HighPrice,
		"lowPrice":   data.LowPrice,
		"closePrice": data.ClosePrice,
		"volume":     data.Volume,
		"trades":     data.NumberOfTrades,
	}

	if fundingData != nil {
		logFields["fundingRate"] = fundingData.FundingRate
	}

	d.logger.WithFields(logFields).Info("New kline data received")

	// Write to CSV
	if err := d.WriteKlineToCSV(symbol, data, fundingData); err != nil {
		d.logger.Errorf("Failed to write to CSV for %s: %v", symbol, err)
	}

}

// FetchHistoricalDataForSymbol fetches historical data for a single symbol
func (c *BinanceClient) FetchHistoricalDataForSymbol(symbol string, dataProcessor *DataProcessor, wg *sync.WaitGroup, errChan chan<- error) {
	defer wg.Done()

	// Calculate time range for 60 days
	endTime := time.Now()
	startTime := endTime.AddDate(0, 0, -60) // 60 days ago

	// Convert to milliseconds for Binance API
	startTimeMs := startTime.UnixMilli()
	endTimeMs := endTime.UnixMilli()

	c.logger.Infof("[%s] Fetching data from %s to %s (%d days)",
		symbol,
		startTime.Format("2006-01-02 15:04:05"),
		endTime.Format("2006-01-02 15:04:05"), 60)

	// Initialize CSV writer for this symbol
	if err := dataProcessor.InitializeCSVWriter(symbol); err != nil {
		errChan <- fmt.Errorf("failed to initialize CSV for %s: %v", symbol, err)
		return
	}

	// We'll fetch funding rates on-demand for each time period
	// to ensure we get the correct historical funding rate for each minute

	// Fetch data in chunks of 1000 records (Binance API limit)
	currentStartTime := startTimeMs
	batchCount := 0
	totalRecords := 0

	for currentStartTime < endTimeMs {
		batchCount++
		currentEndTime := currentStartTime + (1000 * 60 * 1000) // 1000 minutes in milliseconds
		if currentEndTime > endTimeMs {
			currentEndTime = endTimeMs
		}

		c.logger.Infof("[%s] Fetching batch %d (from %s to %s)",
			symbol, batchCount,
			time.UnixMilli(currentStartTime).Format("2006-01-02 15:04:05"),
			time.UnixMilli(currentEndTime).Format("2006-01-02 15:04:05"))

		klines, err := c.GetKlineData(symbol, "1m", 1000, &currentStartTime, &currentEndTime)
		if err != nil {
			c.logger.Errorf("[%s] Failed to fetch data (batch %d): %v", symbol, batchCount, err)
			currentStartTime = currentEndTime
			continue
		}

		c.logger.Infof("[%s] Fetched %d klines (batch %d)", symbol, len(klines), batchCount)
		totalRecords += len(klines)

		// Process and write to CSV with funding rates
		for _, kline := range klines {
			// Get the funding rate that was in effect at this kline's time
			fundingRate, err := c.GetFundingRateForTime(symbol, kline.CloseTime)
			if err != nil {
				c.logger.Warnf("[%s] Failed to get funding rate for time %d: %v", symbol, kline.CloseTime, err)
				// Continue without funding rate
				dataProcessor.ProcessKlineData(symbol, kline, nil)
			} else {
				dataProcessor.ProcessKlineData(symbol, kline, fundingRate)
			}
		}

		// Move to next batch
		currentStartTime = currentEndTime
	}

	c.logger.Infof("[%s] Completed fetching historical data: %d total records", symbol, totalRecords)
}

// abs returns the absolute value of an int64
func abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

// FetchHistoricalData fetches historical data for the last 60 days using multiple goroutines
func (c *BinanceClient) FetchHistoricalData(symbols []string, dataProcessor *DataProcessor) error {
	var wg sync.WaitGroup
	errChan := make(chan error, len(symbols))

	c.logger.Infof("Starting multi-threaded data fetching for %d symbols", len(symbols))

	// Start a goroutine for each symbol
	for _, symbol := range symbols {
		wg.Add(1)
		go c.FetchHistoricalDataForSymbol(symbol, dataProcessor, &wg, errChan)
	}

	// Wait for all goroutines to complete
	wg.Wait()
	close(errChan)

	// Check for any errors
	for err := range errChan {
		if err != nil {
			c.logger.Errorf("Error in data fetching: %v", err)
		}
	}

	return nil
}

func main() {
	// Load environment variables
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using default configuration")
	}

	// Set up logging
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})

	// Fetch all USDT futures symbols from Binance
	logger.Info("Fetching all USDT futures symbols from Binance...")

	// Get exchange info to fetch all symbols
	exchangeInfoURL := "https://fapi.binance.com/fapi/v1/exchangeInfo"
	resp, err := http.Get(exchangeInfoURL)
	if err != nil {
		logger.Errorf("Failed to fetch exchange info: %v", err)
		return
	}
	defer resp.Body.Close()

	var exchangeInfo struct {
		Symbols []struct {
			Symbol     string `json:"symbol"`
			Status     string `json:"status"`
			QuoteAsset string `json:"quoteAsset"`
		} `json:"symbols"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&exchangeInfo); err != nil {
		logger.Errorf("Failed to decode exchange info: %v", err)
		return
	}

	// Filter for USDT symbols that are trading
	var symbols []string
	for _, symbol := range exchangeInfo.Symbols {
		if symbol.QuoteAsset == "USDT" && symbol.Status == "TRADING" {
			symbols = append(symbols, symbol.Symbol)
		}
	}

	logger.Infof("Found %d USDT futures symbols", len(symbols))

	// Optional: Log first few symbols for verification
	if len(symbols) > 0 {
		logger.Infof("Sample symbols: %v", symbols[:min(5, len(symbols))])
	}

	// Create clients
	binanceClient := NewBinanceClient()
	dataProcessor := NewDataProcessor()

	// Fetch 60 days of historical data using multiple threads
	logger.Info("Fetching 60 days of historical kline data with funding rates using multi-threading...")
	if err := binanceClient.FetchHistoricalData(symbols, dataProcessor); err != nil {
		logger.Errorf("Failed to fetch historical data: %v", err)
	}

	logger.Info("Historical data fetching completed. CSV files have been created in the 'data' directory.")

	// Close CSV files
	dataProcessor.CloseCSVFiles()

	logger.Info("Data fetching completed successfully!")
}
