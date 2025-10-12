# GK RealBet

Real-time betting data processor built with Google Genkit AI framework.

## Features

- 🔄 Real-time WebSocket monitoring of BSC blockchain
- 🎯 Smart contract event processing (BetBull/BetBear)
- 🤖 AI-powered data processing with Genkit flows
- 💾 Optimized database operations with connection pooling
- ⚡ High-performance caching with LRU cache

## Architecture

This service processes real-time betting events from a smart contract on BSC and stores them in PostgreSQL using Genkit flows for AI-driven data processing.

## Installation

```bash
npm install
```

## Configuration

Copy `.env.example` to `.env` and configure:

```bash
cp .env.example .env
```

Required environment variables:
- `DATABASE_URL`: PostgreSQL connection string
- `WSS_URL`: WebSocket RPC endpoint for BSC
- `CONTRACT_ADDR`: Smart contract address

## Usage

### Development
```bash
npm start
```

### Monitor with Genkit UI
```bash
genkit start
```
Then open http://localhost:4000 to view flow traces and metrics.

## Deployment

### Railway
This service is optimized for Railway deployment:

1. Push to GitHub
2. Connect Railway to your repository
3. Set environment variables in Railway dashboard
4. Deploy

### Environment Variables for Production
- `DATABASE_URL`: Railway PostgreSQL URL
- `WSS_URL`: Production WSS endpoint
- `CONTRACT_ADDR`: Production contract address

## Project Structure

- `gk_realbet.js`: Main application with Genkit flows
- `abi.json`: Smart contract ABI
- `package.json`: Dependencies and scripts

## Dependencies

- **genkit**: AI framework for flow management
- **ethers**: Ethereum/BSC blockchain interaction
- **pg**: PostgreSQL client with connection pooling
- **dotenv**: Environment variable management

## Flow Description

1. **WebSocket Connection**: Listens to BSC blockchain events
2. **Event Processing**: Captures BetBull/BetBear events
3. **Data Transformation**: Converts blockchain data to database format
4. **Genkit Flow Execution**: AI-powered data validation and storage
5. **Database Storage**: Stores processed data in `realbet` table

## Error Handling

- Automatic WebSocket reconnection
- Database connection recovery
- Genkit flow error tracking
- Block timestamp caching for performance

## Monitoring

Use Genkit Developer UI to monitor:
- Flow execution times
- Success/failure rates
- Real-time event processing
- Database operation performance