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
MqlDateTime stm;

string Tickers[] = { "ABEV3","ALPA4","ALSO3","ARZZ3","ASAI3","AZUL4","B3SA3","BBAS3","BBDC3",
                     "BBDC4","BBSE3","BEEF3","BPAC11","BPAN4","BRAP4","BRFS3","BRKM5","CCRO3",
                     "CIEL3","CMIG4","CMIN3","COGN3","CPFE3","CPLE6","CRFB3","CSAN3","CSNA3",
                     "CVCB3","CYRE3","DXCO3","ECOR3","EGIE3","ELET3","ELET6","EMBR3","ENBR3",
                     "ENEV3","ENGI11","EQTL3","EZTC3","FLRY3","GGBR4","GOAU4","GOLL4","HAPV3",
                     "HYPE3","IGTI11","ITSA4","ITUB4","JBSS3","KLBN11","LREN3","LWSA3","MGLU3",
                     "MRFG3","MRVE3","MULT3","NTCO3","PCAR3","PETR3","PETR4","PETZ3","PRIO3",
                     "QUAL3","RADL3","RAIL3","RAIZ4","RDOR3","RENT3","RRRP3","SANB11","SBSP3",
                     "SLCE3","SMTO3","SOMA3","SUZB3","TAEE11","TIMS3","TOTS3","UGPA3","USIM5",
                     "VALE3","VBBR3","VIIA3","VIVT3","WEGE3","YDUQ3"};
                     
int QuantityOfTickers = ArraySize(Tickers);
int OnOff = 0;
ENUM_TIMEFRAMES tframe[] = {PERIOD_M1};

//+------------------------------------------------------------------+
//| Expert initialization function                                   |
//+------------------------------------------------------------------+

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
   
   while(stm.sec>=2){
      Comment("Waiting for 1 minute -> Time: ",stm.hour,":",stm.min,":",stm.sec," - Time in Minutes: ",stm.hour*60+stm.min);
      TimeCurrent(stm);
      OnOff = 1;
   }
   
   if(OnOff == 1){
      string rowBuild = "";  
      string stringMonth = (string)stm.mon;
      string stringDay = (string)stm.day;
      string stringHour = (string)stm.hour;
      string stringMinute = (string)stm.min;
      string stringSecond = (string)stm.sec;
     
      if(stm.mon<10) stringMonth = "0"+(string)stm.mon;
      if(stm.day<10) stringDay = "0"+(string)stm.day;
      if(stm.hour<10) stringHour = "0"+(string)stm.hour;
      if(stm.min<10) stringMinute = "0"+(string)stm.min;
      if(stm.sec<10) stringSecond = "0"+(string)stm.sec;
     
      string viewTime = (string)stm.year+"."+stringMonth+"."+stringDay+"-"+stringHour+"."+stringMinute+"."+stringSecond;  
      string MySpreadsheet = viewTime + ".csv";
   
      for (k=QuantityOfTickers-1; k>-1; k--){
         CopyRates(Tickers[k],tframe[0],0,1,rates);
         rowBuild = "\n" + (string)rates[0].time + "-" + Tickers[k] + "," + (string)rates[0].time + "," + Tickers[k] + "," + (string)rates[0].open + "," + (string)rates[0].high + "," + (string)rates[0].low + "," + (string)rates[0].close + "," + (string)rates[0].real_volume + "," + (string)rates[0].spread + "," + (string)rates[0].tick_volume + rowBuild;
      }
      rowBuild = "ID" + "," + "DateTime" + "," + "Ticker" + "," + "Open" + "," + "High" + "," + "Low" + "," + "Close" + "," + "RealVolume" + "," + "Spread" + "," + "TickVolume" + rowBuild;
      int mySpreadsheetHandle=FileOpen(MySpreadsheet,FILE_READ|FILE_WRITE|FILE_CSV|FILE_ANSI);
      FileSeek(mySpreadsheetHandle,0,SEEK_SET);
      FileWrite(mySpreadsheetHandle,rowBuild);
      FileClose(mySpreadsheetHandle);
      OnOff = 0;
   }
   Comment("Waiting 2 seconds -> Time: ",stm.hour,":",stm.min,":",stm.sec," - Time in Minutes: ",stm.hour*60+stm.min);
}