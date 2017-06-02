library(rredis)
library(RTrade)
library(TTR)
library(candlesticks)
library(log4r)
library(dplyr)
options(scipen=999)


args.commandline=commandArgs(trailingOnly=TRUE)
if(length(args.commandline)>0){
        args<-args.commandline
}

# args<-c("2","swing01","3")
# args[1] is a flag for model building. 0=> Build Model, 1=> Generate Signals in Production 2=> Backtest and BootStrap 4=>Save BOD Signals to Redis
# args[2] is the strategy name
# args[3] is the redisdatabase
redisConnect()
redisSelect(1)
if(length(args)>1){
        static<-redisHGetAll(toupper(args[2]))
}else{
        static<-redisHGetAll("HARAMI01")
}

newargs<-unlist(strsplit(static$args,","))
if(length(args)<=1 && length(newargs>1)){
        args<-newargs
}
redisClose()


kWriteToRedis <- as.logical(static$WriteToRedis)
kGetMarketData<-as.logical(static$GetMarketData)
kUseFutures<-as.logical(static$UseFutures)
kUseSystemDate<-as.logical(static$UseSystemDate)
kDataCutOffBefore<-static$DataCutOffBefore
kBackTestStartDate<-static$BackTestStartDate
kBackTestEndDate<-static$BackTestEndDate
kFNODataFolder <- static$FNODataFolder
kNiftyDataFolder <- static$NiftyDataFolder
kTimeZone <- static$TimeZone
kBrokerage<-as.numeric(static$SingleLegBrokerageAsPercentOfValue)/100
kExchangeMargin<-as.numeric(static$ExchangeMargin)
kPerContractBrokerage=as.numeric(static$SingleLegBrokerageAsValuePerContract)
kMaxContracts=as.numeric(static$MaxContracts)
kHomeDirectory=static$HomeDirectory
kLogFile=static$LogFile
kExclusionsFile=static$ExclusionsFile
setwd(kHomeDirectory)
strategyname = args[2]
redisDB = args[3]
kBackTestEndDate=strftime(adjust("India",as.Date(kBackTestEndDate, tz = kTimeZone),bdc=2),"%Y-%m-%d")
kBackTestStartDate=strftime(adjust("India",as.Date(kBackTestStartDate, tz = kTimeZone),bdc=0),"%Y-%m-%d")

logger <- create.logger()
logfile(logger) <- kLogFile
level(logger) <- 'INFO'
levellog(logger, "INFO", "Starting BOD Scan")

redisConnect()
redisSelect(2)
a<-unlist(redisSMembers("splits")) # get values from redis in a vector
tmp <- (strsplit(a, split="_")) # convert vector to list
k<-lengths(tmp) # expansion size for each list element
allvalues<-unlist(tmp) # convert list to vector
splits <- data.frame(date=1:length(a), symbol=1:length(a),oldshares=1:length(a),newshares=1:length(a),reason=rep("",length(a)),stringsAsFactors = FALSE)
for(i in 1:length(a)) {
        for(j in 1:k[i]){
                runsum=cumsum(k)[i]
                splits[i, j] <- allvalues[runsum-k[i]+j]
        }
}
splits$date=as.POSIXct(splits$date,format="%Y%m%d",tz="Asia/Kolkata")
splits$oldshares<-as.numeric(splits$oldshares)
splits$newshares<-as.numeric(splits$newshares)
trades<-NULL
#update symbol change
a<-unlist(redisSMembers("symbolchange")) # get values from redis in a vector
tmp <- (strsplit(a, split="_")) # convert vector to list
k<-lengths(tmp) # expansion size for each list element
allvalues<-unlist(tmp) # convert list to vector
symbolchange <- data.frame(date=rep("",length(a)), key=rep("",length(a)),newsymbol=rep("",length(a)),stringsAsFactors = FALSE)
for(i in 1:length(a)) {
        for(j in 1:k[i]){
                runsum=cumsum(k)[i]
                symbolchange[i, j] <- allvalues[runsum-k[i]+j]
        }
}
symbolchange$date=as.POSIXct(symbolchange$date,format="%Y%m%d",tz="Asia/Kolkata")
symbolchange$key = gsub("[^0-9A-Za-z/-]", "", symbolchange$key)
symbolchange$newsymbol = gsub("[^0-9A-Za-z/-]", "", symbolchange$newsymbol)
redisClose()

niftysymbols <-
        createIndexConstituents(2, "nifty50", threshold = "2013-01-01")
folots <- createFNOSize(2, "contractsize", threshold = "2013-01-01")

invalidsymbols = numeric()
endtime = format(Sys.time(), format = "%Y-%m-%d %H:%M:%S")
#enddate=as.Date(endtime)
#niftysymbols=niftysymbols[which(niftysymbols$enddate>=enddate),]
if(!is.null(kExclusionsFile)){
        exclusions<-unlist(read.csv(kExclusionsFile,header=FALSE,stringsAsFactors = FALSE))    
        indicestoremove=match(exclusions,niftysymbols$symbol)
        niftysymbols<-niftysymbols[-indicestoremove,]
}

signals <- data.frame()
allmd <- list()
print("Processing Long Harami...")
for (i in 1:nrow(niftysymbols)) {
        #for (i in 1:10) {
        symbol = niftysymbols$symbol[i]
        if (file.exists(paste(kNiftyDataFolder, symbol, ".Rdata", sep = ""))) {
                load(paste(kNiftyDataFolder, symbol, ".Rdata", sep = "")) #loads md
                md <- md[md$date >= kDataCutOffBefore, ]
                if (nrow(md) > 100) {
                        out<-convertToXTS(md)
                        md$eligible = ifelse(as.Date(md$date) >= niftysymbols[i, c("startdate")] & as.Date(md$date) <= niftysymbols[i, c("enddate")],1,0)
                        md$short<-md$eligible==1 & CSPHarami(out)$Bear.Harami==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend<1
                        md$sell<-md$eligible==1 & CSPHarami(out)$Bear.Harami==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend<1
                        md$buy<-md$eligible==1 & CSPHarami(out)$Bull.Harami==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend>-1
                        md$cover<-md$eligible==1 & CSPHarami(out)$Bull.Harami==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend>-1
                        
                        # md$short<-md$eligible==1 & CSPDoji(out)$GravestoneDoji==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend>=0
                        # md$sell<-md$eligible==1 & CSPDoji(out)$GravestoneDoji==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend>=0
                        # md$buy<-md$eligible==1 & CSPDoji(out)$DragonflyDoji==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend<=0
                        # md$cover<-md$eligible==1 & CSPDoji(out)$DragonflyDoji==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend<=0
                        # 
                        # md$short<-md$eligible==1 & CSPInvertedHammer(out)==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend<0
                        # md$sell<-md$eligible==1 & CSPInvertedHammer(out)==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend<0
                        # md$buy<-md$eligible==1 & CSPHammer(out)==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend>0
                        # md$cover<-md$eligible==1 & CSPHammer(out)==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend>0
                        
                        ####### 2. Generate Buy/Sell Arrays ##########
                        md$buy = ExRem(md$buy,md$sell)
                        md$sell = ExRem(md$sell,md$buy)
                        md$short = ExRem(md$short,md$cover)
                        md$cover = ExRem(md$cover,md$short)
                        BarsSinceBuy = BarsSince(md$buy)
                        BarsSinceShort = BarsSince(md$short)
                        md$sell=ifelse(BarsSinceBuy>5,1,md$sell)
                        md$cover=ifelse(BarsSinceShort>5,1,md$cover)
                        md$sell = ExRem(md$sell,md$buy)
                        md$cover = ExRem(md$cover,md$short)
                        md$CurrentRSI = RSI(md$asettle, 14)
                        md$positionscore = 100 - md$CurrentRSI + (md$asettle - EMA(md$asettle, 20)) * 100 / md$asettle
                        md$buyprice = md$settle
                        md$sellprice = md$settle
                        md$shortprice = md$settle
                        md$coverprice = md$settle
                        md <-  md[md$date >= kBackTestStartDate & md$date <= kBackTestEndDate, ]
                        allmd[[i]] <- md
                        signals <-
                                rbind(md[, c(
                                        "date",
                                        "open",
                                        "high",
                                        "low",
                                        "settle",
                                        "buy",
                                        "sell",
                                        "short",
                                        "cover",
                                        "buyprice",
                                        "shortprice",
                                        "sellprice",
                                        "coverprice",
                                        "positionscore",
                                        "symbol"
                                )], signals)
                } else{
                        invalidsymbols <- c(invalidsymbols, i)
                }
        }
}

signals <- na.omit(signals)
signals$aclose <- signals$settle
signals$asettle <- signals$settle
signals$aopen<-signals$open
signals$ahigh<-signals$high
signals$alow<-signals$low

if(nrow(signals)>0){
        dates <- unique(signals[order(signals$date), c("date")])
        multisymbol<-function(signals,dates){
                out=NULL
                for(i in 1:length(unique(signals$symbol))){
                        #            temp<-ProcessPositionScoreShort(signals[signals$symbol==unique(signals$symbol)[i],],5,dates)
                        temp<-ProcessPositionScore(signals[signals$symbol==unique(signals$symbol)[i],],5,dates)
                        out<-rbind(out,temp)
                }
                out
        }
        a <- multisymbol(signals,dates)
        processedsignals <- ApplySLTP(a, rep(a$asettle*0.10, nrow(a)),rep(a$asettle, nrow(a)),preventReplacement=TRUE)
        processedsignals <- processedsignals[order(processedsignals$date), ]
        processedsignals$currentmonthexpiry <- as.Date(sapply(processedsignals$date, getExpiryDate), tz = kTimeZone)
        nextexpiry <- as.Date(sapply(
                as.Date(processedsignals$currentmonthexpiry + 20, tz = kTimeZone),
                getExpiryDate), tz = kTimeZone)
        processedsignals$entrycontractexpiry <- as.Date(ifelse(
                businessDaysBetween("India",as.Date(processedsignals$date, tz = kTimeZone),processedsignals$currentmonthexpiry) <= 3,
                nextexpiry,processedsignals$currentmonthexpiry),tz = kTimeZone)
        
        processedsignals<-getClosestStrikeUniverse(processedsignals,kFNODataFolder,kNiftyDataFolder,kTimeZone)
        signals<-processedsignals
        multisymbol<-function(uniquesymbols,df,fnodatafolder,equitydatafolder){
                out=NULL
                for(i in 1:length(unique(df$symbol))){
                        if(!kUseFutures){
                                temp<-optionTradeSignalsLongOnly(df[df$symbol==unique(df$symbol)[i],],kFNODataFolder,kNiftyDataFolder,rollover=TRUE)
                        }else{
                                temp<-futureTradeSignals(df[df$symbol==unique(df$symbol)[i],],kFNODataFolder,kNiftyDataFolder,rollover=TRUE)
                        }
                        out<-rbind(out,temp)
                }
                out
        }
        
        optionSignals<-multisymbol(unique(signals$symbol),signals,fnodatafolder,equitydatafolder)
        optionSignals<-optionSignals[with(optionSignals,order(date,symbol,buy,sell)),]
        
        #signals[intersect(grep("ULTRACEMCO",signals$symbol),union(which(signals$buy>=1),which(signals$sell>=1))),]
        
        trades <- GenerateTrades(optionSignals)
        trades <- trades[order(trades$entrytime), ]
        
        getcontractsize <- function (x, size) {
                a <- size[size$startdate <= as.Date(x) & size$enddate >= as.Date(x), ]
                if (nrow(a) > 0) {
                        a <- head(a, 1)
                }
                if (nrow(a) > 0) {
                        return(a$contractsize)
                } else
                        return(0)
                
        }
        
        if(nrow(trades)>0){
                trades$reason<-"HaramiLong"
                trades$size=NULL
                novalue=strptime(NA_character_,"%Y-%m-%d")
                for(i in 1:nrow(trades)){
                        symbolsvector=unlist(strsplit(trades$symbol[i],"_"))
                        allsize = folots[folots$symbol == symbolsvector[1], ]
                        trades$size[i]=getcontractsize(trades$entrytime[i],allsize)*kMaxContracts
                        if(as.numeric(trades$exittime[i])==0){
                                trades$exittime[i]=novalue
                        }
                }
                
                trades$brokerage <- 2*kPerContractBrokerage / (trades$entryprice*trades$size)
                trades$netpercentprofit <- trades$percentprofit - trades$brokerage
                trades$absolutepnl<-NA_real_
                BizDayBacktestEnd=adjust("India",min(Sys.Date(),
                                                     as.Date(kBackTestEndDate, tz = kTimeZone)),bdc=2)
                for (t in 1:nrow(trades)) {
                        expirydate = as.Date(unlist(strsplit(trades$symbol[t], "_"))[3], "%Y%m%d", tz =
                                                     kTimeZone)
                        if (trades$exitprice[t]==0 &&
                            (
                                    (expirydate > min(Sys.Date(),
                                                      as.Date(kBackTestEndDate, tz = kTimeZone)))
                            )){
                                symbolsvector=unlist(strsplit(trades$symbol[t],"_"))
                                load(paste(kFNODataFolder,symbolsvector[3],"/", trades$symbol[t], ".Rdata", sep = ""))
                                index=which(as.Date(md$date,tz=kTimeZone)==BizDayBacktestEnd)
                                if(length(index)==1){
                                        trades$exitprice[t] = md$settle[index]
                                }else{
                                        trades$exitprice[t] = tail(md$settle,1)
                                }
                        }
                        trades$absolutepnl[t] = (trades$exitprice[t] - trades$entryprice[t] -
                                                         (2*kPerContractBrokerage / trades$size[t])) * trades$size[t]
                }
                
                
                trades$absolutepnl[which(is.na(trades$absolutepnl))]<-0
        }
        
}

# Short Harami
signals <- data.frame()
allmd <- list()
print("Processing Short Harami...")

for (i in 1:nrow(niftysymbols)) {
        #for (i in 1:10) {
        symbol = niftysymbols$symbol[i]
        if (file.exists(paste(kNiftyDataFolder, symbol, ".Rdata", sep = ""))) {
                load(paste(kNiftyDataFolder, symbol, ".Rdata", sep = "")) #loads md
                md <- md[md$date >= kDataCutOffBefore, ]
                if (nrow(md) > 100) {
                        out<-convertToXTS(md)
                        md$eligible = ifelse(as.Date(md$date) >= niftysymbols[i, c("startdate")] & as.Date(md$date) <= niftysymbols[i, c("enddate")],1,0)
                        md$short<-md$eligible==1 & CSPHarami(out)$Bear.Harami==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend<1
                        md$sell<-md$eligible==1 & CSPHarami(out)$Bear.Harami==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend<1
                        md$buy<-md$eligible==1 & CSPHarami(out)$Bull.Harami==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend>-1
                        md$cover<-md$eligible==1 & CSPHarami(out)$Bull.Harami==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend>-1
                        
                        # md$short<-md$eligible==1 & CSPDoji(out)$GravestoneDoji==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend>=0
                        # md$sell<-md$eligible==1 & CSPDoji(out)$GravestoneDoji==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend>=0
                        # md$buy<-md$eligible==1 & CSPDoji(out)$DragonflyDoji==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend<=0
                        # md$cover<-md$eligible==1 & CSPDoji(out)$DragonflyDoji==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend<=0
                        # 
                        # md$short<-md$eligible==1 & CSPInvertedHammer(out)==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend<0
                        # md$sell<-md$eligible==1 & CSPInvertedHammer(out)==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend<0
                        # md$buy<-md$eligible==1 & CSPHammer(out)==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend>0
                        # md$cover<-md$eligible==1 & CSPHammer(out)==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend>0
                        
                        ####### 2. Generate Buy/Sell Arrays ##########
                        md$buy = ExRem(md$buy,md$sell)
                        md$sell = ExRem(md$sell,md$buy)
                        md$short = ExRem(md$short,md$cover)
                        md$cover = ExRem(md$cover,md$short)
                        BarsSinceBuy = BarsSince(md$buy)
                        BarsSinceShort = BarsSince(md$short)
                        md$sell=ifelse(BarsSinceBuy>5,1,md$sell)
                        md$cover=ifelse(BarsSinceShort>5,1,md$cover)
                        md$sell = ExRem(md$sell,md$buy)
                        md$cover = ExRem(md$cover,md$short)
                        md$CurrentRSI = RSI(md$asettle, 14)
                        md$positionscore = 100 - md$CurrentRSI + (md$asettle - EMA(md$asettle, 20)) * 100 / md$asettle
                        md$buyprice = md$settle
                        md$sellprice = md$settle
                        md$shortprice = md$settle
                        md$coverprice = md$settle
                        md <-  md[md$date >= kBackTestStartDate & md$date <= kBackTestEndDate, ]
                        allmd[[i]] <- md
                        signals <-
                                rbind(md[, c(
                                        "date",
                                        "open",
                                        "high",
                                        "low",
                                        "settle",
                                        "buy",
                                        "sell",
                                        "short",
                                        "cover",
                                        "buyprice",
                                        "shortprice",
                                        "sellprice",
                                        "coverprice",
                                        "positionscore",
                                        "symbol"
                                )], signals)
                } else{
                        invalidsymbols <- c(invalidsymbols, i)
                }
        }
}

signals <- na.omit(signals)
signals$aclose <- signals$settle
signals$asettle <- signals$settle
signals$aopen<-signals$open
signals$ahigh<-signals$high
signals$alow<-signals$low

if(nrow(signals)>0){
        dates <- unique(signals[order(signals$date), c("date")])
        multisymbol<-function(signals,dates){
                out=NULL
                for(i in 1:length(unique(signals$symbol))){
                        temp<-ProcessPositionScoreShort(signals[signals$symbol==unique(signals$symbol)[i],],5,dates)
                        #temp<-ProcessPositionScore(signals[signals$symbol==unique(signals$symbol)[i],],5,dates)
                        out<-rbind(out,temp)
                }
                out
        }
        a <- multisymbol(signals,dates)
        processedsignals <- ApplySLTP(a, rep(a$asettle*0.10, nrow(a)),rep(a$asettle, nrow(a)))
        processedsignals <- processedsignals[order(processedsignals$date), ]
        processedsignals$currentmonthexpiry <- as.Date(sapply(processedsignals$date, getExpiryDate), tz = kTimeZone)
        nextexpiry <- as.Date(sapply(
                as.Date(processedsignals$currentmonthexpiry + 20, tz = kTimeZone),
                getExpiryDate), tz = kTimeZone)
        processedsignals$entrycontractexpiry <- as.Date(ifelse(
                businessDaysBetween("India",as.Date(processedsignals$date, tz = kTimeZone),processedsignals$currentmonthexpiry) <= 3,
                nextexpiry,processedsignals$currentmonthexpiry),tz = kTimeZone)
        
        processedsignals<-getClosestStrikeUniverse(processedsignals,kFNODataFolder,kNiftyDataFolder,kTimeZone)
        signals<-processedsignals
        multisymbol<-function(uniquesymbols,df,fnodatafolder,equitydatafolder){
                out=NULL
                for(i in 1:length(unique(df$symbol))){
                        if(!kUseFutures){
                                temp<-optionTradeSignalsLongOnly(df[df$symbol==unique(df$symbol)[i],],kFNODataFolder,kNiftyDataFolder,rollover=TRUE)
                        }else{
                                temp<-futureTradeSignals(df[df$symbol==unique(df$symbol)[i],],kFNODataFolder,kNiftyDataFolder,rollover=TRUE)
                        }
                        out<-rbind(out,temp)
                }
                out
        }
        
        optionSignals<-multisymbol(unique(signals$symbol),signals,fnodatafolder,equitydatafolder)
        optionSignals<-optionSignals[with(optionSignals,order(date,symbol,buy,sell)),]
        
        #signals[intersect(grep("ULTRACEMCO",signals$symbol),union(which(signals$buy>=1),which(signals$sell>=1))),]
        
        shorttrades <- GenerateTrades(optionSignals)
        shorttrades <- shorttrades[order(shorttrades$entrytime), ]
        
        getcontractsize <- function (x, size) {
                a <- size[size$startdate <= as.Date(x) & size$enddate >= as.Date(x), ]
                if (nrow(a) > 0) {
                        a <- head(a, 1)
                }
                if (nrow(a) > 0) {
                        return(a$contractsize)
                } else
                        return(0)
                
        }
        
        if(nrow(shorttrades)>0){
                shorttrades$reason<-"HaramiShort"
                shorttrades$size=NULL
                novalue=strptime(NA_character_,"%Y-%m-%d")
                for(i in 1:nrow(shorttrades)){
                        symbolsvector=unlist(strsplit(shorttrades$symbol[i],"_"))
                        allsize = folots[folots$symbol == symbolsvector[1], ]
                        shorttrades$size[i]=getcontractsize(shorttrades$entrytime[i],allsize)*kMaxContracts
                        if(as.numeric(shorttrades$exittime[i])==0){
                                shorttrades$exittime[i]=novalue
                        }
                }
                
                shorttrades$brokerage <- 2*kPerContractBrokerage / (shorttrades$entryprice*shorttrades$size)
                shorttrades$netpercentprofit <- shorttrades$percentprofit - shorttrades$brokerage
                shorttrades$absolutepnl<-NA_real_
                BizDayBacktestEnd=adjust("India",min(Sys.Date(),
                                                     as.Date(kBackTestEndDate, tz = kTimeZone)),bdc=2)
                for (t in 1:nrow(shorttrades)) {
                        expirydate = as.Date(unlist(strsplit(shorttrades$symbol[t], "_"))[3], "%Y%m%d", tz =
                                                     kTimeZone)
                        if (shorttrades$exitprice[t]==0 &&
                            (
                                    (expirydate > min(Sys.Date(),
                                                      as.Date(kBackTestEndDate, tz = kTimeZone)))
                            )){
                                symbolsvector=unlist(strsplit(shorttrades$symbol[t],"_"))
                                load(paste(kFNODataFolder,symbolsvector[3],"/", shorttrades$symbol[t], ".Rdata", sep = ""))
                                index=which(as.Date(md$date,tz=kTimeZone)==BizDayBacktestEnd)
                                if(length(index)==1){
                                        shorttrades$exitprice[t] = md$settle[index]
                                }else{
                                        shorttrades$exitprice[t] = tail(md$settle,1)
                                }
                        }
                        shorttrades$absolutepnl[t] = (shorttrades$exitprice[t] - shorttrades$entryprice[t] -
                                                              (2*kPerContractBrokerage / shorttrades$size[t])) * shorttrades$size[t]
                }
                
                
                shorttrades$absolutepnl[which(is.na(shorttrades$absolutepnl))]<-0
        }
        trades<-rbind(trades,shorttrades)
        trades <- trades[order(trades$entrytime), ]
        
}

#Long Doji
signals <- data.frame()
allmd <- list()
print("Processing Long Doji...")

for (i in 1:nrow(niftysymbols)) {
        #for (i in 1:10) {
        symbol = niftysymbols$symbol[i]
        if (file.exists(paste(kNiftyDataFolder, symbol, ".Rdata", sep = ""))) {
                load(paste(kNiftyDataFolder, symbol, ".Rdata", sep = "")) #loads md
                md <- md[md$date >= kDataCutOffBefore, ]
                if (nrow(md) > 100) {
                        out<-convertToXTS(md)
                        md$eligible = ifelse(as.Date(md$date) >= niftysymbols[i, c("startdate")] & as.Date(md$date) <= niftysymbols[i, c("enddate")],1,0)
                        # md$short<-md$eligible==1 & CSPHarami(out)$Bear.Harami==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend<1
                        # md$sell<-md$eligible==1 & CSPHarami(out)$Bear.Harami==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend<1
                        # md$buy<-md$eligible==1 & CSPHarami(out)$Bull.Harami==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend>-1
                        # md$cover<-md$eligible==1 & CSPHarami(out)$Bull.Harami==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend>-1
                        
                        md$short<-md$eligible==1 & CSPDoji(out)$GravestoneDoji==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend>=0
                        md$sell<-md$eligible==1 & CSPDoji(out)$GravestoneDoji==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend>=0
                        md$buy<-md$eligible==1 & CSPDoji(out)$DragonflyDoji==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend<=0
                        md$cover<-md$eligible==1 & CSPDoji(out)$DragonflyDoji==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend<=0
                        # 
                        # md$short<-md$eligible==1 & CSPInvertedHammer(out)==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend<0
                        # md$sell<-md$eligible==1 & CSPInvertedHammer(out)==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend<0
                        # md$buy<-md$eligible==1 & CSPHammer(out)==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend>0
                        # md$cover<-md$eligible==1 & CSPHammer(out)==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend>0
                        
                        ####### 2. Generate Buy/Sell Arrays ##########
                        md$buy = ExRem(md$buy,md$sell)
                        md$sell = ExRem(md$sell,md$buy)
                        md$short = ExRem(md$short,md$cover)
                        md$cover = ExRem(md$cover,md$short)
                        BarsSinceBuy = BarsSince(md$buy)
                        BarsSinceShort = BarsSince(md$short)
                        md$sell=ifelse(BarsSinceBuy>5,1,md$sell)
                        md$cover=ifelse(BarsSinceShort>5,1,md$cover)
                        md$sell = ExRem(md$sell,md$buy)
                        md$cover = ExRem(md$cover,md$short)
                        md$CurrentRSI = RSI(md$asettle, 14)
                        md$positionscore = 100 - md$CurrentRSI + (md$asettle - EMA(md$asettle, 20)) * 100 / md$asettle
                        md$buyprice = md$settle
                        md$sellprice = md$settle
                        md$shortprice = md$settle
                        md$coverprice = md$settle
                        md <-  md[md$date >= kBackTestStartDate & md$date <= kBackTestEndDate, ]
                        allmd[[i]] <- md
                        signals <-
                                rbind(md[, c(
                                        "date",
                                        "open",
                                        "high",
                                        "low",
                                        "settle",
                                        "buy",
                                        "sell",
                                        "short",
                                        "cover",
                                        "buyprice",
                                        "shortprice",
                                        "sellprice",
                                        "coverprice",
                                        "positionscore",
                                        "symbol"
                                )], signals)
                } else{
                        invalidsymbols <- c(invalidsymbols, i)
                }
        }
}

signals <- na.omit(signals)
signals$aclose <- signals$settle
signals$asettle <- signals$settle
signals$aopen<-signals$open
signals$ahigh<-signals$high
signals$alow<-signals$low

if(nrow(signals)>0){
        dates <- unique(signals[order(signals$date), c("date")])
        multisymbol<-function(signals,dates){
                out=NULL
                for(i in 1:length(unique(signals$symbol))){
                        #temp<-ProcessPositionScoreShort(signals[signals$symbol==unique(signals$symbol)[i],],5,dates)
                        temp<-ProcessPositionScore(signals[signals$symbol==unique(signals$symbol)[i],],5,dates)
                        out<-rbind(out,temp)
                }
                out
        }
        a <- multisymbol(signals,dates)
        processedsignals <- ApplySLTP(a, rep(a$asettle*0.10, nrow(a)),rep(a$asettle, nrow(a)))
        processedsignals <- processedsignals[order(processedsignals$date), ]
        processedsignals$currentmonthexpiry <- as.Date(sapply(processedsignals$date, getExpiryDate), tz = kTimeZone)
        nextexpiry <- as.Date(sapply(
                as.Date(processedsignals$currentmonthexpiry + 20, tz = kTimeZone),
                getExpiryDate), tz = kTimeZone)
        processedsignals$entrycontractexpiry <- as.Date(ifelse(
                businessDaysBetween("India",as.Date(processedsignals$date, tz = kTimeZone),processedsignals$currentmonthexpiry) <= 3,
                nextexpiry,processedsignals$currentmonthexpiry),tz = kTimeZone)
        
        processedsignals<-getClosestStrikeUniverse(processedsignals,kFNODataFolder,kNiftyDataFolder,kTimeZone)
        signals<-processedsignals
        multisymbol<-function(uniquesymbols,df,fnodatafolder,equitydatafolder){
                out=NULL
                for(i in 1:length(unique(df$symbol))){
                        if(!kUseFutures){
                                temp<-optionTradeSignalsLongOnly(df[df$symbol==unique(df$symbol)[i],],kFNODataFolder,kNiftyDataFolder,rollover=TRUE)
                        }else{
                                temp<-futureTradeSignals(df[df$symbol==unique(df$symbol)[i],],kFNODataFolder,kNiftyDataFolder,rollover=TRUE)
                        }
                        out<-rbind(out,temp)
                }
                out
        }
        
        optionSignals<-multisymbol(unique(signals$symbol),signals,fnodatafolder,equitydatafolder)
        optionSignals<-optionSignals[with(optionSignals,order(date,symbol,buy,sell)),]
        
        #signals[intersect(grep("ULTRACEMCO",signals$symbol),union(which(signals$buy>=1),which(signals$sell>=1))),]
        
        shorttrades <- GenerateTrades(optionSignals)
        shorttrades <- shorttrades[order(shorttrades$entrytime), ]
        
        getcontractsize <- function (x, size) {
                a <- size[size$startdate <= as.Date(x) & size$enddate >= as.Date(x), ]
                if (nrow(a) > 0) {
                        a <- head(a, 1)
                }
                if (nrow(a) > 0) {
                        return(a$contractsize)
                } else
                        return(0)
                
        }
        
        if(nrow(shorttrades)>0){
                shorttrades$reason<-"DojiLong"
                shorttrades$size=NULL
                novalue=strptime(NA_character_,"%Y-%m-%d")
                for(i in 1:nrow(shorttrades)){
                        symbolsvector=unlist(strsplit(shorttrades$symbol[i],"_"))
                        allsize = folots[folots$symbol == symbolsvector[1], ]
                        shorttrades$size[i]=getcontractsize(shorttrades$entrytime[i],allsize)*kMaxContracts
                        if(as.numeric(shorttrades$exittime[i])==0){
                                shorttrades$exittime[i]=novalue
                        }
                }
                
                shorttrades$brokerage <- 2*kPerContractBrokerage / (shorttrades$entryprice*shorttrades$size)
                shorttrades$netpercentprofit <- shorttrades$percentprofit - shorttrades$brokerage
                shorttrades$absolutepnl<-NA_real_
                BizDayBacktestEnd=adjust("India",min(Sys.Date(),
                                                     as.Date(kBackTestEndDate, tz = kTimeZone)),bdc=2)
                for (t in 1:nrow(shorttrades)) {
                        expirydate = as.Date(unlist(strsplit(shorttrades$symbol[t], "_"))[3], "%Y%m%d", tz =
                                                     kTimeZone)
                        if (shorttrades$exitprice[t]==0 &&
                            (
                                    (expirydate > min(Sys.Date(),
                                                      as.Date(kBackTestEndDate, tz = kTimeZone)))
                            )){
                                symbolsvector=unlist(strsplit(shorttrades$symbol[t],"_"))
                                load(paste(kFNODataFolder,symbolsvector[3],"/", shorttrades$symbol[t], ".Rdata", sep = ""))
                                index=which(as.Date(md$date,tz=kTimeZone)==BizDayBacktestEnd)
                                if(length(index)==1){
                                        shorttrades$exitprice[t] = md$settle[index]
                                }else{
                                        shorttrades$exitprice[t] = tail(md$settle,1)
                                }
                        }
                        shorttrades$absolutepnl[t] = (shorttrades$exitprice[t] - shorttrades$entryprice[t] -
                                                              (2*kPerContractBrokerage / shorttrades$size[t])) * shorttrades$size[t]
                }
                
                
                shorttrades$absolutepnl[which(is.na(shorttrades$absolutepnl))]<-0
        }
        trades<-rbind(trades,shorttrades)
        trades <- trades[order(trades$entrytime), ]
        
}

#Short Doji
signals <- data.frame()
allmd <- list()
print("Processing Short Doji...")

for (i in 1:nrow(niftysymbols)) {
        #for (i in 1:10) {
        symbol = niftysymbols$symbol[i]
        if (file.exists(paste(kNiftyDataFolder, symbol, ".Rdata", sep = ""))) {
                load(paste(kNiftyDataFolder, symbol, ".Rdata", sep = "")) #loads md
                md <- md[md$date >= kDataCutOffBefore, ]
                if (nrow(md) > 100) {
                        out<-convertToXTS(md)
                        md$eligible = ifelse(as.Date(md$date) >= niftysymbols[i, c("startdate")] & as.Date(md$date) <= niftysymbols[i, c("enddate")],1,0)
                        # md$short<-md$eligible==1 & CSPHarami(out)$Bear.Harami==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend<1
                        # md$sell<-md$eligible==1 & CSPHarami(out)$Bear.Harami==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend<1
                        # md$buy<-md$eligible==1 & CSPHarami(out)$Bull.Harami==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend>-1
                        # md$cover<-md$eligible==1 & CSPHarami(out)$Bull.Harami==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend>-1
                        
                        md$short<-md$eligible==1 & CSPDoji(out)$GravestoneDoji==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend>=0
                        md$sell<-md$eligible==1 & CSPDoji(out)$GravestoneDoji==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend>=0
                        md$buy<-md$eligible==1 & CSPDoji(out)$DragonflyDoji==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend<=0
                        md$cover<-md$eligible==1 & CSPDoji(out)$DragonflyDoji==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend<=0
                        # 
                        # md$short<-md$eligible==1 & CSPInvertedHammer(out)==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend<0
                        # md$sell<-md$eligible==1 & CSPInvertedHammer(out)==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend<0
                        # md$buy<-md$eligible==1 & CSPHammer(out)==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend>0
                        # md$cover<-md$eligible==1 & CSPHammer(out)==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend>0
                        
                        ####### 2. Generate Buy/Sell Arrays ##########
                        md$buy = ExRem(md$buy,md$sell)
                        md$sell = ExRem(md$sell,md$buy)
                        md$short = ExRem(md$short,md$cover)
                        md$cover = ExRem(md$cover,md$short)
                        BarsSinceBuy = BarsSince(md$buy)
                        BarsSinceShort = BarsSince(md$short)
                        md$sell=ifelse(BarsSinceBuy>5,1,md$sell)
                        md$cover=ifelse(BarsSinceShort>5,1,md$cover)
                        md$sell = ExRem(md$sell,md$buy)
                        md$cover = ExRem(md$cover,md$short)
                        md$CurrentRSI = RSI(md$asettle, 14)
                        md$positionscore = 100 - md$CurrentRSI + (md$asettle - EMA(md$asettle, 20)) * 100 / md$asettle
                        md$buyprice = md$settle
                        md$sellprice = md$settle
                        md$shortprice = md$settle
                        md$coverprice = md$settle
                        md <-  md[md$date >= kBackTestStartDate & md$date <= kBackTestEndDate, ]
                        allmd[[i]] <- md
                        signals <-
                                rbind(md[, c(
                                        "date",
                                        "open",
                                        "high",
                                        "low",
                                        "settle",
                                        "buy",
                                        "sell",
                                        "short",
                                        "cover",
                                        "buyprice",
                                        "shortprice",
                                        "sellprice",
                                        "coverprice",
                                        "positionscore",
                                        "symbol"
                                )], signals)
                } else{
                        invalidsymbols <- c(invalidsymbols, i)
                }
        }
}

signals <- na.omit(signals)
signals$aclose <- signals$settle
signals$asettle <- signals$settle
signals$aopen<-signals$open
signals$ahigh<-signals$high
signals$alow<-signals$low

if(nrow(signals)>0){
        dates <- unique(signals[order(signals$date), c("date")])
        multisymbol<-function(signals,dates){
                out=NULL
                for(i in 1:length(unique(signals$symbol))){
                        temp<-ProcessPositionScoreShort(signals[signals$symbol==unique(signals$symbol)[i],],5,dates)
                        #temp<-ProcessPositionScore(signals[signals$symbol==unique(signals$symbol)[i],],5,dates)
                        out<-rbind(out,temp)
                }
                out
        }
        a <- multisymbol(signals,dates)
        processedsignals <- ApplySLTP(a, rep(a$asettle*0.10, nrow(a)),rep(a$asettle, nrow(a)))
        processedsignals <- processedsignals[order(processedsignals$date), ]
        processedsignals$currentmonthexpiry <- as.Date(sapply(processedsignals$date, getExpiryDate), tz = kTimeZone)
        nextexpiry <- as.Date(sapply(
                as.Date(processedsignals$currentmonthexpiry + 20, tz = kTimeZone),
                getExpiryDate), tz = kTimeZone)
        processedsignals$entrycontractexpiry <- as.Date(ifelse(
                businessDaysBetween("India",as.Date(processedsignals$date, tz = kTimeZone),processedsignals$currentmonthexpiry) <= 3,
                nextexpiry,processedsignals$currentmonthexpiry),tz = kTimeZone)
        
        processedsignals<-getClosestStrikeUniverse(processedsignals,kFNODataFolder,kNiftyDataFolder,kTimeZone)
        signals<-processedsignals
        multisymbol<-function(uniquesymbols,df,fnodatafolder,equitydatafolder){
                out=NULL
                for(i in 1:length(unique(df$symbol))){
                        if(!kUseFutures){
                                temp<-optionTradeSignalsLongOnly(df[df$symbol==unique(df$symbol)[i],],kFNODataFolder,kNiftyDataFolder,rollover=TRUE)
                        }else{
                                temp<-futureTradeSignals(df[df$symbol==unique(df$symbol)[i],],kFNODataFolder,kNiftyDataFolder,rollover=TRUE)
                        }
                        out<-rbind(out,temp)
                }
                out
        }
        
        optionSignals<-multisymbol(unique(signals$symbol),signals,fnodatafolder,equitydatafolder)
        optionSignals<-optionSignals[with(optionSignals,order(date,symbol,buy,sell)),]
        
        #signals[intersect(grep("ULTRACEMCO",signals$symbol),union(which(signals$buy>=1),which(signals$sell>=1))),]
        
        shorttrades <- GenerateTrades(optionSignals)
        shorttrades <- shorttrades[order(shorttrades$entrytime), ]
        
        getcontractsize <- function (x, size) {
                a <- size[size$startdate <= as.Date(x) & size$enddate >= as.Date(x), ]
                if (nrow(a) > 0) {
                        a <- head(a, 1)
                }
                if (nrow(a) > 0) {
                        return(a$contractsize)
                } else
                        return(0)
                
        }
        
        if(nrow(shorttrades)>0){
                shorttrades$reason<-"DojiShort"
                shorttrades$size=NULL
                novalue=strptime(NA_character_,"%Y-%m-%d")
                for(i in 1:nrow(shorttrades)){
                        symbolsvector=unlist(strsplit(shorttrades$symbol[i],"_"))
                        allsize = folots[folots$symbol == symbolsvector[1], ]
                        shorttrades$size[i]=getcontractsize(shorttrades$entrytime[i],allsize)*kMaxContracts
                        if(as.numeric(shorttrades$exittime[i])==0){
                                shorttrades$exittime[i]=novalue
                        }
                }
                
                shorttrades$brokerage <- 2*kPerContractBrokerage / (shorttrades$entryprice*shorttrades$size)
                shorttrades$netpercentprofit <- shorttrades$percentprofit - shorttrades$brokerage
                shorttrades$absolutepnl<-NA_real_
                BizDayBacktestEnd=adjust("India",min(Sys.Date(),
                                                     as.Date(kBackTestEndDate, tz = kTimeZone)),bdc=2)
                for (t in 1:nrow(shorttrades)) {
                        expirydate = as.Date(unlist(strsplit(shorttrades$symbol[t], "_"))[3], "%Y%m%d", tz =
                                                     kTimeZone)
                        if (shorttrades$exitprice[t]==0 &&
                            (
                                    (expirydate > min(Sys.Date(),
                                                      as.Date(kBackTestEndDate, tz = kTimeZone)))
                            )){
                                symbolsvector=unlist(strsplit(shorttrades$symbol[t],"_"))
                                load(paste(kFNODataFolder,symbolsvector[3],"/", shorttrades$symbol[t], ".Rdata", sep = ""))
                                index=which(as.Date(md$date,tz=kTimeZone)==BizDayBacktestEnd)
                                if(length(index)==1){
                                        shorttrades$exitprice[t] = md$settle[index]
                                }else{
                                        shorttrades$exitprice[t] = tail(md$settle,1)
                                }
                        }
                        shorttrades$absolutepnl[t] = (shorttrades$exitprice[t] - shorttrades$entryprice[t] -
                                                              (2*kPerContractBrokerage / shorttrades$size[t])) * shorttrades$size[t]
                }
                
                
                shorttrades$absolutepnl[which(is.na(shorttrades$absolutepnl))]<-0
        }
        trades<-rbind(trades,shorttrades)
        trades <- trades[order(trades$entrytime), ]
        
}

#Long Hammer
signals <- data.frame()
allmd <- list()
print("Processing Long Hammer...")

for (i in 1:nrow(niftysymbols)) {
        #for (i in 1:10) {
        symbol = niftysymbols$symbol[i]
        if (file.exists(paste(kNiftyDataFolder, symbol, ".Rdata", sep = ""))) {
                load(paste(kNiftyDataFolder, symbol, ".Rdata", sep = "")) #loads md
                md <- md[md$date >= kDataCutOffBefore, ]
                if (nrow(md) > 100) {
                        out<-convertToXTS(md)
                        md$eligible = ifelse(as.Date(md$date) >= niftysymbols[i, c("startdate")] & as.Date(md$date) <= niftysymbols[i, c("enddate")],1,0)
                        # md$short<-md$eligible==1 & CSPHarami(out)$Bear.Harami==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend<1
                        # md$sell<-md$eligible==1 & CSPHarami(out)$Bear.Harami==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend<1
                        # md$buy<-md$eligible==1 & CSPHarami(out)$Bull.Harami==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend>-1
                        # md$cover<-md$eligible==1 & CSPHarami(out)$Bull.Harami==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend>-1
                        
                        # md$short<-md$eligible==1 & CSPDoji(out)$GravestoneDoji==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend>=0
                        # md$sell<-md$eligible==1 & CSPDoji(out)$GravestoneDoji==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend>=0
                        # md$buy<-md$eligible==1 & CSPDoji(out)$DragonflyDoji==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend<=0
                        # md$cover<-md$eligible==1 & CSPDoji(out)$DragonflyDoji==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend<=0
                        # 
                        md$short<-md$eligible==1 & CSPInvertedHammer(out)==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend<0
                        md$sell<-md$eligible==1 & CSPInvertedHammer(out)==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend<0
                        md$buy<-md$eligible==1 & CSPHammer(out)==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend>0
                        md$cover<-md$eligible==1 & CSPHammer(out)==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend>0
                        
                        ####### 2. Generate Buy/Sell Arrays ##########
                        md$buy = ExRem(md$buy,md$sell)
                        md$sell = ExRem(md$sell,md$buy)
                        md$short = ExRem(md$short,md$cover)
                        md$cover = ExRem(md$cover,md$short)
                        BarsSinceBuy = BarsSince(md$buy)
                        BarsSinceShort = BarsSince(md$short)
                        md$sell=ifelse(BarsSinceBuy>5,1,md$sell)
                        md$cover=ifelse(BarsSinceShort>5,1,md$cover)
                        md$sell = ExRem(md$sell,md$buy)
                        md$cover = ExRem(md$cover,md$short)
                        md$CurrentRSI = RSI(md$asettle, 14)
                        md$positionscore = 100 - md$CurrentRSI + (md$asettle - EMA(md$asettle, 20)) * 100 / md$asettle
                        md$buyprice = md$settle
                        md$sellprice = md$settle
                        md$shortprice = md$settle
                        md$coverprice = md$settle
                        md <-  md[md$date >= kBackTestStartDate & md$date <= kBackTestEndDate, ]
                        allmd[[i]] <- md
                        signals <-
                                rbind(md[, c(
                                        "date",
                                        "open",
                                        "high",
                                        "low",
                                        "settle",
                                        "buy",
                                        "sell",
                                        "short",
                                        "cover",
                                        "buyprice",
                                        "shortprice",
                                        "sellprice",
                                        "coverprice",
                                        "positionscore",
                                        "symbol"
                                )], signals)
                } else{
                        invalidsymbols <- c(invalidsymbols, i)
                }
        }
}

signals <- na.omit(signals)
signals$aclose <- signals$settle
signals$asettle <- signals$settle
signals$aopen<-signals$open
signals$ahigh<-signals$high
signals$alow<-signals$low

if(nrow(signals)>0){
        dates <- unique(signals[order(signals$date), c("date")])
        multisymbol<-function(signals,dates){
                out=NULL
                for(i in 1:length(unique(signals$symbol))){
                        #                temp<-ProcessPositionScoreShort(signals[signals$symbol==unique(signals$symbol)[i],],5,dates)
                        temp<-ProcessPositionScore(signals[signals$symbol==unique(signals$symbol)[i],],5,dates)
                        out<-rbind(out,temp)
                }
                out
        }
        a <- multisymbol(signals,dates)
        processedsignals <- ApplySLTP(a, rep(a$asettle*0.10, nrow(a)),rep(a$asettle, nrow(a)))
        processedsignals <- processedsignals[order(processedsignals$date), ]
        processedsignals$currentmonthexpiry <- as.Date(sapply(processedsignals$date, getExpiryDate), tz = kTimeZone)
        nextexpiry <- as.Date(sapply(
                as.Date(processedsignals$currentmonthexpiry + 20, tz = kTimeZone),
                getExpiryDate), tz = kTimeZone)
        processedsignals$entrycontractexpiry <- as.Date(ifelse(
                businessDaysBetween("India",as.Date(processedsignals$date, tz = kTimeZone),processedsignals$currentmonthexpiry) <= 3,
                nextexpiry,processedsignals$currentmonthexpiry),tz = kTimeZone)
        
        processedsignals<-getClosestStrikeUniverse(processedsignals,kFNODataFolder,kNiftyDataFolder,kTimeZone)
        signals<-processedsignals
        multisymbol<-function(uniquesymbols,df,fnodatafolder,equitydatafolder){
                out=NULL
                for(i in 1:length(unique(df$symbol))){
                        if(!kUseFutures){
                                temp<-optionTradeSignalsLongOnly(df[df$symbol==unique(df$symbol)[i],],kFNODataFolder,kNiftyDataFolder,rollover=TRUE)
                        }else{
                                temp<-futureTradeSignals(df[df$symbol==unique(df$symbol)[i],],kFNODataFolder,kNiftyDataFolder,rollover=TRUE)
                        }
                        out<-rbind(out,temp)
                }
                out
        }
        
        optionSignals<-multisymbol(unique(signals$symbol),signals,fnodatafolder,equitydatafolder)
        optionSignals<-optionSignals[with(optionSignals,order(date,symbol,buy,sell)),]
        
        #signals[intersect(grep("ULTRACEMCO",signals$symbol),union(which(signals$buy>=1),which(signals$sell>=1))),]
        
        shorttrades <- GenerateTrades(optionSignals)
        shorttrades <- shorttrades[order(shorttrades$entrytime), ]
        
        getcontractsize <- function (x, size) {
                a <- size[size$startdate <= as.Date(x) & size$enddate >= as.Date(x), ]
                if (nrow(a) > 0) {
                        a <- head(a, 1)
                }
                if (nrow(a) > 0) {
                        return(a$contractsize)
                } else
                        return(0)
                
        }
        
        if(nrow(shorttrades)>0){
                shorttrades$reason<-"HammerLong"
                shorttrades$size=NULL
                novalue=strptime(NA_character_,"%Y-%m-%d")
                for(i in 1:nrow(shorttrades)){
                        symbolsvector=unlist(strsplit(shorttrades$symbol[i],"_"))
                        allsize = folots[folots$symbol == symbolsvector[1], ]
                        shorttrades$size[i]=getcontractsize(shorttrades$entrytime[i],allsize)*kMaxContracts
                        if(as.numeric(shorttrades$exittime[i])==0){
                                shorttrades$exittime[i]=novalue
                        }
                }
                
                shorttrades$brokerage <- 2*kPerContractBrokerage / (shorttrades$entryprice*shorttrades$size)
                shorttrades$netpercentprofit <- shorttrades$percentprofit - shorttrades$brokerage
                shorttrades$absolutepnl<-NA_real_
                BizDayBacktestEnd=adjust("India",min(Sys.Date(),
                                                     as.Date(kBackTestEndDate, tz = kTimeZone)),bdc=2)
                for (t in 1:nrow(shorttrades)) {
                        expirydate = as.Date(unlist(strsplit(shorttrades$symbol[t], "_"))[3], "%Y%m%d", tz =
                                                     kTimeZone)
                        if (shorttrades$exitprice[t]==0 &&
                            (
                                    (expirydate > min(Sys.Date(),
                                                      as.Date(kBackTestEndDate, tz = kTimeZone)))
                            )){
                                symbolsvector=unlist(strsplit(shorttrades$symbol[t],"_"))
                                load(paste(kFNODataFolder,symbolsvector[3],"/", shorttrades$symbol[t], ".Rdata", sep = ""))
                                index=which(as.Date(md$date,tz=kTimeZone)==BizDayBacktestEnd)
                                if(length(index)==1){
                                        shorttrades$exitprice[t] = md$settle[index]
                                }else{
                                        shorttrades$exitprice[t] = tail(md$settle,1)
                                }
                        }
                        shorttrades$absolutepnl[t] = (shorttrades$exitprice[t] - shorttrades$entryprice[t] -
                                                              (2*kPerContractBrokerage / shorttrades$size[t])) * shorttrades$size[t]
                }
                
                
                shorttrades$absolutepnl[which(is.na(shorttrades$absolutepnl))]<-0
        }
        trades<-rbind(trades,shorttrades)
        trades <- trades[order(trades$entrytime), ]
        
}

#Short Hammer
signals <- data.frame()
allmd <- list()
print("Processing Short Harami...")

for (i in 1:nrow(niftysymbols)) {
        #for (i in 1:10) {
        symbol = niftysymbols$symbol[i]
        if (file.exists(paste(kNiftyDataFolder, symbol, ".Rdata", sep = ""))) {
                load(paste(kNiftyDataFolder, symbol, ".Rdata", sep = "")) #loads md
                md <- md[md$date >= kDataCutOffBefore, ]
                if (nrow(md) > 100) {
                        out<-convertToXTS(md)
                        md$eligible = ifelse(as.Date(md$date) >= niftysymbols[i, c("startdate")] & as.Date(md$date) <= niftysymbols[i, c("enddate")],1,0)
                        # md$short<-md$eligible==1 & CSPHarami(out)$Bear.Harami==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend<1
                        # md$sell<-md$eligible==1 & CSPHarami(out)$Bear.Harami==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend<1
                        # md$buy<-md$eligible==1 & CSPHarami(out)$Bull.Harami==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend>-1
                        # md$cover<-md$eligible==1 & CSPHarami(out)$Bull.Harami==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend>-1
                        
                        # md$short<-md$eligible==1 & CSPDoji(out)$GravestoneDoji==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend>=0
                        # md$sell<-md$eligible==1 & CSPDoji(out)$GravestoneDoji==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend>=0
                        # md$buy<-md$eligible==1 & CSPDoji(out)$DragonflyDoji==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend<=0
                        # md$cover<-md$eligible==1 & CSPDoji(out)$DragonflyDoji==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend<=0
                        # 
                        md$short<-md$eligible==1 & CSPInvertedHammer(out)==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend<0
                        md$sell<-md$eligible==1 & CSPInvertedHammer(out)==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend<0
                        md$buy<-md$eligible==1 & CSPHammer(out)==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend>0
                        md$cover<-md$eligible==1 & CSPHammer(out)==TRUE & md$avolume>TTR::SMA(md$avolume,30) & Trend(md$date,md$open,md$high,md$close)$trend>0
                        
                        ####### 2. Generate Buy/Sell Arrays ##########
                        md$buy = ExRem(md$buy,md$sell)
                        md$sell = ExRem(md$sell,md$buy)
                        md$short = ExRem(md$short,md$cover)
                        md$cover = ExRem(md$cover,md$short)
                        BarsSinceBuy = BarsSince(md$buy)
                        BarsSinceShort = BarsSince(md$short)
                        md$sell=ifelse(BarsSinceBuy>5,1,md$sell)
                        md$cover=ifelse(BarsSinceShort>5,1,md$cover)
                        md$sell = ExRem(md$sell,md$buy)
                        md$cover = ExRem(md$cover,md$short)
                        md$CurrentRSI = RSI(md$asettle, 14)
                        md$positionscore = 100 - md$CurrentRSI + (md$asettle - EMA(md$asettle, 20)) * 100 / md$asettle
                        md$buyprice = md$settle
                        md$sellprice = md$settle
                        md$shortprice = md$settle
                        md$coverprice = md$settle
                        md <-  md[md$date >= kBackTestStartDate & md$date <= kBackTestEndDate, ]
                        allmd[[i]] <- md
                        signals <-
                                rbind(md[, c(
                                        "date",
                                        "open",
                                        "high",
                                        "low",
                                        "settle",
                                        "buy",
                                        "sell",
                                        "short",
                                        "cover",
                                        "buyprice",
                                        "shortprice",
                                        "sellprice",
                                        "coverprice",
                                        "positionscore",
                                        "symbol"
                                )], signals)
                } else{
                        invalidsymbols <- c(invalidsymbols, i)
                }
        }
}

signals <- na.omit(signals)
signals$aclose <- signals$settle
signals$asettle <- signals$settle
signals$aopen<-signals$open
signals$ahigh<-signals$high
signals$alow<-signals$low

if(nrow(signals)>0){
        dates <- unique(signals[order(signals$date), c("date")])
        multisymbol<-function(signals,dates){
                out=NULL
                for(i in 1:length(unique(signals$symbol))){
                        temp<-ProcessPositionScoreShort(signals[signals$symbol==unique(signals$symbol)[i],],5,dates)
                        #temp<-ProcessPositionScore(signals[signals$symbol==unique(signals$symbol)[i],],5,dates)
                        out<-rbind(out,temp)
                }
                out
        }
        a <- multisymbol(signals,dates)
        processedsignals <- ApplySLTP(a, rep(a$asettle*0.10, nrow(a)),rep(a$asettle, nrow(a)))
        processedsignals <- processedsignals[order(processedsignals$date), ]
        processedsignals$currentmonthexpiry <- as.Date(sapply(processedsignals$date, getExpiryDate), tz = kTimeZone)
        nextexpiry <- as.Date(sapply(
                as.Date(processedsignals$currentmonthexpiry + 20, tz = kTimeZone),
                getExpiryDate), tz = kTimeZone)
        processedsignals$entrycontractexpiry <- as.Date(ifelse(
                businessDaysBetween("India",as.Date(processedsignals$date, tz = kTimeZone),processedsignals$currentmonthexpiry) <= 3,
                nextexpiry,processedsignals$currentmonthexpiry),tz = kTimeZone)
        
        processedsignals<-getClosestStrikeUniverse(processedsignals,kFNODataFolder,kNiftyDataFolder,kTimeZone)
        signals<-processedsignals
        multisymbol<-function(uniquesymbols,df,fnodatafolder,equitydatafolder){
                out=NULL
                for(i in 1:length(unique(df$symbol))){
                        if(!kUseFutures){
                                temp<-optionTradeSignalsLongOnly(df[df$symbol==unique(df$symbol)[i],],kFNODataFolder,kNiftyDataFolder,rollover=TRUE)
                        }else{
                                temp<-futureTradeSignals(df[df$symbol==unique(df$symbol)[i],],kFNODataFolder,kNiftyDataFolder,rollover=TRUE)
                        }
                        out<-rbind(out,temp)
                }
                out
        }
        
        optionSignals<-multisymbol(unique(signals$symbol),signals,fnodatafolder,equitydatafolder)
        optionSignals<-optionSignals[with(optionSignals,order(date,symbol,buy,sell)),]
        
        #signals[intersect(grep("ULTRACEMCO",signals$symbol),union(which(signals$buy>=1),which(signals$sell>=1))),]
        
        shorttrades <- GenerateTrades(optionSignals)
        shorttrades <- shorttrades[order(shorttrades$entrytime), ]
        
        getcontractsize <- function (x, size) {
                a <- size[size$startdate <= as.Date(x) & size$enddate >= as.Date(x), ]
                if (nrow(a) > 0) {
                        a <- head(a, 1)
                }
                if (nrow(a) > 0) {
                        return(a$contractsize)
                } else
                        return(0)
                
        }
        
        if(nrow(shorttrades)>0){
                shorttrades$reason<-"HammerShort"
                shorttrades$size=NULL
                novalue=strptime(NA_character_,"%Y-%m-%d")
                for(i in 1:nrow(shorttrades)){
                        symbolsvector=unlist(strsplit(shorttrades$symbol[i],"_"))
                        allsize = folots[folots$symbol == symbolsvector[1], ]
                        shorttrades$size[i]=getcontractsize(shorttrades$entrytime[i],allsize)*kMaxContracts
                        if(as.numeric(shorttrades$exittime[i])==0){
                                shorttrades$exittime[i]=novalue
                        }
                }
                
                shorttrades$brokerage <- 2*kPerContractBrokerage / (shorttrades$entryprice*shorttrades$size)
                shorttrades$netpercentprofit <- shorttrades$percentprofit - shorttrades$brokerage
                shorttrades$absolutepnl<-NA_real_
                BizDayBacktestEnd=adjust("India",min(Sys.Date(),
                                                     as.Date(kBackTestEndDate, tz = kTimeZone)),bdc=2)
                for (t in 1:nrow(shorttrades)) {
                        expirydate = as.Date(unlist(strsplit(shorttrades$symbol[t], "_"))[3], "%Y%m%d", tz =
                                                     kTimeZone)
                        if (shorttrades$exitprice[t]==0 &&
                            (
                                    (expirydate > min(Sys.Date(),
                                                      as.Date(kBackTestEndDate, tz = kTimeZone)))
                            )){
                                symbolsvector=unlist(strsplit(shorttrades$symbol[t],"_"))
                                load(paste(kFNODataFolder,symbolsvector[3],"/", shorttrades$symbol[t], ".Rdata", sep = ""))
                                index=which(as.Date(md$date,tz=kTimeZone)==BizDayBacktestEnd)
                                if(length(index)==1){
                                        shorttrades$exitprice[t] = md$settle[index]
                                }else{
                                        shorttrades$exitprice[t] = tail(md$settle,1)
                                }
                        }
                        shorttrades$absolutepnl[t] = (shorttrades$exitprice[t] - shorttrades$entryprice[t] -
                                                              (2*kPerContractBrokerage / shorttrades$size[t])) * shorttrades$size[t]
                }
                
                
                shorttrades$absolutepnl[which(is.na(shorttrades$absolutepnl))]<-0
        }
        trades<-rbind(trades,shorttrades)
        trades <- trades[order(trades$entrytime), ]
        
}


winratio<-sum(trades$absolutepnl>0)/nrow(trades)
absolutepnl<-sum(trades$absolutepnl)
print(paste("abs pnl",absolutepnl,sep=":"))
print(paste("win ratio",winratio*100,sep=":"))
trades.dplyr<-group_by(trades,reason)
trades.summary<-summarise(trades.dplyr,tradecount=n(),profit=sum(absolutepnl),winratio=sum(absolutepnl>0)/n())
print(trades.summary)

########### SAVE SIGNALS TO REDIS #################

entrysize <- 0
exitsize <- 0

if (kUseSystemDate) {
        today = Sys.Date()
} else{
        today = advance("India",dates=Sys.Date(),n=-1,timeUnit = 0,bdc=2)
}
yesterday=advance("India",dates=today,n=-1,timeUnit = 0,bdc=2)

entrycount = which(as.Date(trades$entrytime,tz=kTimeZone) == today)
exitcount = which(as.Date(trades$exittime,tz=kTimeZone) == today)

if (length(exitcount) > 0 && kWriteToRedis && args[1]==1) {
        redisConnect()
        redisSelect(args[3])
        out <- trades[which(as.Date(trades$exittime,tz=kTimeZone) == today),]
        uniquesymbols=NULL
        for (o in 1:nrow(out)) {
                if(length(grep(out[o,"symbol"],uniquesymbols))==0){
                        uniquesymbols[length(uniquesymbols)+1]<-out[o,"symbol"]
                        startingposition = GetCurrentPosition(out[o, "symbol"], trades,position.on=yesterday,trades.till=yesterday)
                        todaytradesize=startingposition-GetCurrentPosition(out[o, "symbol"], trades)
                        redisString = paste(out[o, "symbol"],
                                            todaytradesize,
                                            ifelse(startingposition>0,"SELL","COVER"),
                                            0,
                                            abs(startingposition),
                                            sep = ":")
                        redisRPush(paste("trades", args[2], sep = ":"),
                                   charToRaw(redisString))
                        levellog(logger,
                                 "INFO",
                                 paste(args[2], redisString, sep = ":"))
                }
        }
        redisClose()
}


if (length(entrycount) > 0 && kWriteToRedis && args[1]==1) {
        redisConnect()
        redisSelect(args[3])
        out <- trades[which(as.Date(trades$entrytime,tz=kTimeZone) == today),]
        uniquesymbols=NULL
        for (o in 1:nrow(out)) {
                if(length(grep(out[o,"symbol"],uniquesymbols))==0){
                        uniquesymbols[length(uniquesymbols)+1]<-out[o,"symbol"]
                        startingposition = GetCurrentPosition(out[o, "symbol"], trades,trades.till=yesterday,position.on = today)
                        todaytradesize=GetCurrentPosition(out[o, "symbol"], trades)-startingposition
                        redisString = paste(out[o, "symbol"],
                                            abs(todaytradesize),
                                            ifelse(todaytradesize>0,"BUY","SHORT"),
                                            0,
                                            abs(startingposition),
                                            sep = ":")
                        redisRPush(paste("trades", args[2], sep = ":"),
                                   charToRaw(redisString))
                        levellog(logger,
                                 "INFO",
                                 paste(args[2], redisString, sep = ":"))
                        
                }
                
        }
        redisClose()
}

