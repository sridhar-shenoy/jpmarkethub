# MarketHub Application - High-Performance Market Data Aggregation System

# Problem Statement  
Write a Gateway that conects to two market data publisher
  1. BidAndOffer
  2. LastPrice
     
The application must consume these two sources and publish data to internal JP consumer who will parse the collated data

## Table of Contents
- [Key Features](#key-features)
- [Core Components](#Core-Components)
- [Architecture Design](#architecture-design)
- [Api Specification of the Producers](#api-specifications)
- [Api Specification For Internal Consumers](#api-specifications)
- [Performance Metrics](#performance-metrics)

---
## Key Features
### 🔒 Thread-Safe Lock-Free Implementation
- Lock-free data structures and busy-wait patterns
### 🧹 Zero Garbage Architecture
- Pre-allocated Ring buffer reuse
- Application does not create object runtime 
## Architecture Design

![System Architecture](https://github.com/user-attachments/assets/c91d180f-81f9-4003-93c8-71c6c80cd64f)  
*Real-Time Data Processing Pipeline with Zero GC Overhead*


## Core Components
```plaintext
+----------------+       +-----------------+       +-------------------+
|  Exchange Data | ---> | MarketHub App    | ---> | Internal consumer  |
+----------------+       +-----------------+       +-------------------+
                              ▲
                              │
                      +-----------------+
                      | Ring Buffer Pool |
                      +-----------------+
```

## Api Specification of the Producers
1. The **BidAndOffer** is Published on port 9000
```
SequenceNo,BidPrice,OfferPrice;
```
2. The **LastPrice** is Published on Port 9001
```
SequenceNo,LastPrice;
```

## Api Specification For Internal Consumers
Currently All Internal Cosnumers can connect via TCP connection to port **10000** to subscribe to Collated Data
1. The **BidAndOfferLastPrice** is Published wis published as below 
```
SequenceNo,BidPrice,OfferPrice,LastPrice
```
# Archetecture Design 

## MarketHub
1. **Market Hub** is the main application that initiates connections to Multiple Market Data source
  - For Each of these Connection MarketHub assignes a ring Buffer dedicated to this Producer
  - As and when data arrives from this Producer, ring buffer is populated and current valid sequence updated
2. Market Hub exposes multiple ports for any internal clients to connect
   - Whenever a client connects on a port, MarketHub attahes it to a **busy spin thread**
   - this Consumer class will always monitor the Ringbuffer via a volatile variable to process any new data
   - when there is data available, Consumer will collate the data and publish to all connected clients 

# Performance metrics 
> [!NOTE]
>  The metircs were recoreded on the 
> Processor	Intel(R) Core(TM) i7-10700T CPU @ 2.00GHz, 2001 Mhz, 8 Core(s), 16 Logical Processor(s)

##  📌 Zero Garbage Design.
> [!NOTE]
> The MemoryProfile.java class was run to Check Memory consumption of the applicatiom
> The profile results indicate that over prolonged period,

```
Memory = 0 MB for i = [ 85000 ] 
Memory = 0 MB for i = [ 86000 ] 
Memory = 0 MB for i = [ 87000 ] 
Memory = 0 MB for i = [ 88000 ] 
Memory = 0 MB for i = [ 89000 ] 
Memory = 0 MB for i = [ 90000 ] 
Memory = 0 MB for i = [ 91000 ] 
Memory = 0 MB for i = [ 92000 ] 
Memory = 0 MB for i = [ 93000 ] 
Memory = 0 MB for i = [ 94000 ] 
Memory = 0 MB for i = [ 95000 ] 
Memory = 0 MB for i = [ 96000 ] 
Memory = 0 MB for i = [ 97000 ] 
Memory = 0 MB for i = [ 98000 ] 
Memory = 0 MB for i = [ 99000 ] 
```

##  📌 Low Latency Architecture.
> [!NOTE]
> The PerformanceProfile.java class was run to Check latency profile of the applicatiom
> The profile results indicate that from the time the data is published till its consumed by consumers. latency ,

```
PERF -- DIFF[ 0 ] latency [ 0 ms ] timeToPublishData [ 11314 ms ] 
PERF -- DIFF[ 0 ] latency [ -2 ms ] timeToPublishData [ 11172 ms ] 
PERF -- DIFF[ 0 ] latency [ -2 ms ] timeToPublishData [ 11219 ms ] 
PERF -- DIFF[ 0 ] latency [ -1 ms ] timeToPublishData [ 11234 ms ] 
PERF -- DIFF[ 0 ] latency [ 0 ms ] timeToPublishData [ 11287 ms ] 
PERF -- DIFF[ 0 ] latency [ 0 ms ] timeToPublishData [ 11294 ms ] 
PERF -- DIFF[ 0 ] latency [ 0 ms ] timeToPublishData [ 11078 ms ] 
PERF -- DIFF[ 0 ] latency [ 0 ms ] timeToPublishData [ 11010 ms ] 
PERF -- DIFF[ 0 ] latency [ 0 ms ] timeToPublishData [ 11017 ms ] 
```
##  📌 TDD Based Developement .
> [!NOTE]
> Have written enough specification to get a functional working model
> Have made sure the majority of the critica path is covered 

![image](https://github.com/user-attachments/assets/77cc6d0a-6547-4c12-819f-3845a35ed863)



