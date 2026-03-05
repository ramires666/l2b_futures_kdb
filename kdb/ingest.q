/ Minimal kdb+ backend for buffered ingest from Python IPC

.ingest.getenv:{[k;d]
  v:getenv k;
  $[0=count v; d; v]
  };

.ingest.getenvLong:{[k;d]
  v:getenv k;
  $[0=count v; d; "J"$v]
  };

.ingest.dbRoot:.ingest.getenv[`L2B_DB_DIR;"/home/rut/l2b_kdb_db"];
.ingest.persistMs:.ingest.getenvLong[`L2B_PERSIST_MS;5000];
.ingest.compressFlag:lower .ingest.getenv[`L2B_COMPRESS;"1"];
.ingest.compressOn:any .ingest.compressFlag~/:("1";"true";"yes";"on");
.ingest.compressBlockLog2:.ingest.getenvLong[`L2B_COMPRESS_BLOCKLOG2;17];
.ingest.compressAlg:.ingest.getenvLong[`L2B_COMPRESS_ALG;3];
.ingest.compressLevel:.ingest.getenvLong[`L2B_COMPRESS_LEVEL;0];

.ingest.emptyTrades:{
  ([] ts:0#0Np;
      window_ts:0#0Np;
      sym:0#`symbol$();
      trade_id:0#0Nj;
      px:0#0n;
      qty:0#0n;
      is_bm:0#0b;
      evt_ts:0#0Np)
  };

.ingest.emptyL2:{
  ([] ts:0#0Np;
      window_ts:0#0Np;
      sym:0#`symbol$();
      side:0#`symbol$();
      lvl:0#0Ni;
      px:0#0n;
      qty:0#0n;
      evt_ts:0#0Np)
  };

.ingest.trades:.ingest.emptyTrades[];
.ingest.l2:.ingest.emptyL2[];
.ingest.bufTrades:.ingest.emptyTrades[];
.ingest.bufL2:.ingest.emptyL2[];

.ingest.mkDir:{[tbl;d]
  dir:raze (.ingest.dbRoot;"/";string tbl;"/";d);
  system "mkdir -p ",dir;
  dir
  };

.ingest.mkFile:{[tbl]
  d:string .z.D;
  ts:string .z.P;
  ts:ssr[ts;":";"-"];
  ts:ssr[ts;" ";"_"];
  dir:.ingest.mkDir[tbl;d];
  f:raze (dir;"/";ts;".qbin");
  hsym `$f
  };

.ingest.writeFile:{[f;x]
  $[.ingest.compressOn;
    (f;.ingest.compressBlockLog2;.ingest.compressAlg;.ingest.compressLevel) set x;
    f set x]
  };

.ingest.updTrades:{[tsns;winns;sym;tid;px;qty;isbm;evtns]
  if[0=count tsns; :0N];
  symv:$[11h=type sym; sym; `$'sym];
  t:([] ts:`timestamp$tsns;
       window_ts:`timestamp$winns;
       sym:symv;
       trade_id:`long$tid;
       px:`float$px;
       qty:`float$qty;
       is_bm:`boolean$isbm;
       evt_ts:`timestamp$evtns);
  .ingest.trades,:t;
  .ingest.bufTrades,:t;
  :count t
  };

.ingest.updL2:{[tsns;winns;sym;isbid;lvl;px;qty;evtns]
  if[0=count tsns; :0N];
  symv:$[11h=type sym; sym; `$'sym];
  bidv:$[1h=type isbid; isbid; `boolean$isbid];
  t:([] ts:`timestamp$tsns;
       window_ts:`timestamp$winns;
       sym:symv;
       side:?[bidv;`b;`a];
       lvl:`int$lvl;
       px:`float$px;
       qty:`float$qty;
       evt_ts:`timestamp$evtns);
  .ingest.l2,:t;
  .ingest.bufL2,:t;
  :count t
  };

.ingest.persist:{
  if[count .ingest.bufTrades;
    f:.ingest.mkFile[`trades];
    .ingest.writeFile[f;.ingest.bufTrades];
    .ingest.bufTrades:.ingest.emptyTrades[];
    ];
  if[count .ingest.bufL2;
    f:.ingest.mkFile[`l2];
    .ingest.writeFile[f;.ingest.bufL2];
    .ingest.bufL2:.ingest.emptyL2[];
    ];
  :`ok
  };

/ Periodic disk flush
.z.ts:{.ingest.persist[]};
system "t ",string .ingest.persistMs;

show "L2B ingest initialized";
show "dbRoot=",.ingest.dbRoot;
show "persistMs=",string .ingest.persistMs;
show "compressOn=",string .ingest.compressOn;
show "compressSpec=",string (.ingest.compressBlockLog2;.ingest.compressAlg;.ingest.compressLevel);
