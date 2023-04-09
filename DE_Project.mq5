//+------------------------------------------------------------------+
//|                                                   DE_Project.mq5 |
//|                                Copyright 2023, Henrique Felipini |
//|                                     https://github.com/Hfelipini |
//+------------------------------------------------------------------+
#property copyright "Copyright 2023, Henrique Felipini"
#property link      "https://github.com/Hfelipini"
#property version   "1.00"
//+------------------------------------------------------------------+
//| Expert initialization function                                   |
//+------------------------------------------------------------------+
MqlRates rates[];
MqlDateTime stm, stm2;
   
string Tickers[] = {"ABEV3","ALPA4","ALSO3","ARZZ3","ASAI3","AZUL4","B3SA3","BBAS3","BBDC3","BBDC4","BBSE3","BEEF3","BPAC11","BPAN4","BRAP4","BRFS3","BRKM5","CASH3","CCRO3","CIEL3","CMIG4","CMIN3","COGN3","CPFE3","CPLE6","CRFB3","CSAN3","CSNA3","CVCB3","CYRE3","DXCO3","ECOR3","EGIE3","ELET3","ELET6","EMBR3","ENBR3","ENEV3","ENGI11","EQTL3","EZTC3","FLRY3","GGBR4","GOAU4","GOLL4","HAPV3","HYPE3","IGTI11","ITSA4","ITUB4","JBSS3","KLBN11","LREN3","LWSA3","MGLU3","MRFG3","MRVE3","MULT3","NTCO3","PCAR3","PETR3","PETR4","PETZ3","PRIO3","QUAL3","RADL3","RAIL3","RAIZ4","RDOR3","RENT3","RRRP3","SANB11","SBSP3","SLCE3","SMTO3","SOMA3","SUZB3","TAEE11","TIMS3","TOTS3","UGPA3","USIM5","VALE3","VBBR3","VIIA3","VIVT3","WEGE3","YDUQ3"};
int QuantityOfTickers = ArraySize(Tickers);

ENUM_TIMEFRAMES tframe[] = {PERIOD_M1};

int OnInit(){
   ArraySetAsSeries(rates, true);
   return(INIT_SUCCEEDED);
}

//https://www.mql5.com/en/docs/series/copyrates
void OnTick(){

   TimeCurrent(stm);
   int k = 0;
   while(stm.hour*60+stm.min<600||stm.hour*60+stm.min>1015){ 
      Comment("Market closed -> Time: ",stm.hour,":",stm.min,":",stm.sec," - Time in Minutes: ",stm.hour*60+stm.min); 
      TimeCurrent(stm);
   }
   
   while(stm.sec>=3){
      Comment("Waiting for 1 minute -> Time: ",stm.hour,":",stm.min,":",stm.sec); 
      TimeCurrent(stm);
   }
 
   string rowBuild = "";  
   string viewTime = (string)stm.year+"."+(string)stm.mon+"."+(string)stm.day+"-"+(string)stm.hour+"."+(string)stm.min+"."+(string)stm.sec;   
   string viewTimeRow = (string)stm.year+"/"+(string)stm.mon+"/"+(string)stm.day+" "+(string)stm.hour+":"+(string)stm.min+":"+(string)stm.sec;   
   string MySpreadsheet = viewTime + ".csv";
   for (k=QuantityOfTickers-1; k>-1; k--){
      CopyRates(Tickers[k],tframe[0],0,1,rates);
      rowBuild = "\n" + (string)rates[0].time + "-" + Tickers[k] + "," + viewTimeRow + "," + Tickers[k] + "," + (string)rates[0].open + "," + (string)rates[0].high + "," + (string)rates[0].low + "," + (string)rates[0].close + "," + (string)rates[0].real_volume + "," + (string)rates[0].spread + "," + (string)rates[0].tick_volume + "," + (string)rates[0].time + rowBuild;
   }
   rowBuild = "ID" + "," + "DateTime" + "," + "Ticker" + "," + "Open" + "," + "High" + "," + "Low" + "," + "Close" + "," + "RealVolume" + "," + "Spread" + "," + "TickVolume" + "," + "Time" + rowBuild; 
   int mySpreadsheetHandle=FileOpen(MySpreadsheet,FILE_READ|FILE_WRITE|FILE_CSV|FILE_UNICODE);
   FileSeek(mySpreadsheetHandle,0,SEEK_SET);
   FileWrite(mySpreadsheetHandle,rowBuild);
   FileClose(mySpreadsheetHandle);
   TimeCurrent(stm2);
   while(stm2.sec - stm.sec < 3){
      TimeCurrent(stm2);
      Comment("Hold - Waiting 3 seconds: ", stm2.sec);
   }
}