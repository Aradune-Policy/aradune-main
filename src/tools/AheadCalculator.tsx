import { useState, useMemo, useCallback, useEffect } from "react";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Cell, Legend, Line, LineChart, ReferenceLine, ScatterChart, Scatter, ComposedChart, Area } from "recharts";
import { C, FONT, SHADOW } from "../design";

// ═══════════════════════════════════════════════════════════════════
// AHEAD GLOBAL BUDGET CALCULATOR
// ═══════════════════════════════════════════════════════════════════

const cB="#0A2540",cM="#5B21B6",cR="#D93025",cT="#0891B2",cG="#059669",cO="#D97706",cP="#7C3AED";
const TR="all 0.25s ease";
const fmt=(n:number|null|undefined):string=>{if(n==null)return"—";const a=Math.abs(n);return(n<0?"-":"")+(a>=1e9?"$"+(a/1e9).toFixed(2)+"B":a>=1e6?"$"+(a/1e6).toFixed(1)+"M":a>=1e3?"$"+(a/1e3).toFixed(0)+"K":"$"+a.toFixed(0));};
const fP=(n:number|null|undefined):string=>{if(n==null)return"—";return(n>=0?"+":"")+(n*100).toFixed(2)+"%";};

// ═══════════════════════════════════════════════════════════════════
// TYPE DEFINITIONS
// ═══════════════════════════════════════════════════════════════════

interface AheadHospital {
  id:string;nm:string;st:string;co:number;ty:string;beds:number;tch:string;sn:boolean;cah:boolean;
  wi:number;ms:number;bn:number;dp:number;cdi:number;hcc:number;
  bl:{ip:number;op:number;uc:number};
  q:{vbp:number;hrrp:number;hacrp:number;ra:number;pqi:number;ed:number};
  tcoc:{t:number;a:number};cost:number;
  mcd:{ip:number;op:number;supp:{dsh:number;dir:number};bn:number;hedis:{pre:number;ed:number;dia:number;fu:number}};
}
interface StateParams {fmap:number;mcPct:number;ipT:number;opT:number;mt:string;md:string;upl:number;sdoh:number;}
interface WaterfallStep {n:string;v:number;c:number;}
interface McrResult {py:number;cY:number;wT:number;wI:number;wO:number;wU:number;opt:number;pI:number;pO:number;pU:number;pA:number;pV:number;sra:number;qD:number;qB:number;eff:number;tA:number;hb:number;fin:number;ffs:number;delta:number;pct:number;cOn:boolean;wf:WaterfallStep[];}
interface McdResult {py:number;cY:number;wB:number;wI:number;wO:number;sB:number;wT:number;tI:number;tO:number;pT:number;pV:number;sT:number;sdA:number;hC:number;hA:number;mcS:number;fin:number;ffs:number;delta:number;pct:number;uC:number;uOn:boolean;fSh:number;stS:number;wf:WaterfallStep[];}
interface McYearResult {py:number;cY:number;cp10:number;cp25:number;cp50:number;cp75:number;cp90:number;cffs:number;pA:number;spread:number;mVP:number;dVP:number;var5:number;cvar5:number;rho:number;pCAH:number;pUPL:number;rawC:number[];rawM:number[];rawD:number[];}
interface CustomFormData {name:string;state:string;cohort:number;beds:number;teaching:string;safetyNet:boolean;wageIndex:number;benes:number;dualPct:number;cdi:number;hcc:number;ipRev:number;opRev:number;ucRev:number;vbp:number;hrrp:number;hacrp:number;readmit:number;pqi:number;ed:number;tcocT:number;tcocA:number;cahCost:number;mcdIp:number;mcdOp:number;mcdDsh:number;mcdDir:number;mcdBn:number;hPre:number;hEd:number;hDia:number;hFu:number;}
interface Scenario {nm:string;bD:number;mV:number|null;vO:number|null;hO:number|null;aO:number|null;yrs:number;hospId:string;ts:number;}
interface FieldDef {k:string;l:string;g:string;t:string;def:number|string|boolean|(()=>string);ex:number|string|boolean;opts?:(string|number)[];}
type CellValue = string | number | {v:React.ReactNode;s?:React.CSSProperties};

// ═══════════════════════════════════════════════════════════════════
// CMS BENCHMARKS & STATE PARAMS & HOSPITALS
// ═══════════════════════════════════════════════════════════════════

const SP:Record<string,StateParams>={
  MD:{fmap:.50,mcPct:.75,ipT:1.038,opT:1.042,mt:"TCOC",md:"Total Cost of Care",upl:1.50,sdoh:1.04},
  CT:{fmap:.50,mcPct:.85,ipT:1.035,opT:1.040,mt:"VBP",md:"Value-Based Payment",upl:1.45,sdoh:1.03},
  VT:{fmap:.5533,mcPct:.25,ipT:1.032,opT:1.038,mt:"ACO",md:"All-Payer ACO",upl:1.55,sdoh:1.05},
  HI:{fmap:.5156,mcPct:.90,ipT:1.040,opT:1.045,mt:"QUEST",md:"QUEST Integration",upl:1.40,sdoh:1.02},
  NY:{fmap:.50,mcPct:.80,ipT:1.042,opT:1.048,mt:"VBP",md:"VBP Roadmap",upl:1.60,sdoh:1.06},
  RI:{fmap:.5257,mcPct:.70,ipT:1.036,opT:1.041,mt:"ACE",md:"Accountable Entity",upl:1.48,sdoh:1.04},
};
const spGet=(st:string):StateParams=>SP[st]||{fmap:.50,mcPct:.70,ipT:1.036,opT:1.042,mt:"FFS",md:"Fee-for-Service",upl:1.50,sdoh:1.03};

const H:AheadHospital[]=[
  {id:"210009",nm:"Johns Hopkins",st:"MD",co:1,ty:"ACH",beds:1059,tch:"MAJOR",sn:false,cah:false,wi:1.0396,ms:.155,bn:38000,dp:.22,cdi:54,hcc:1.42,bl:{ip:738e6,op:458e6,uc:19e6},q:{vbp:1.018,hrrp:.9921,hacrp:1.0,ra:.123,pqi:.032,ed:.41},tcoc:{t:1380,a:1345},cost:0,mcd:{ip:280e6,op:180e6,supp:{dsh:45e6,dir:12e6},bn:42000,hedis:{pre:.82,ed:.72,dia:.78,fu:.68}}},
  {id:"210002",nm:"Univ of Maryland",st:"MD",co:1,ty:"ACH",beds:757,tch:"MAJOR",sn:true,cah:false,wi:1.0396,ms:.098,bn:24000,dp:.42,cdi:78,hcc:1.55,bl:{ip:442e6,op:266e6,uc:24e6},q:{vbp:.987,hrrp:.9845,hacrp:.99,ra:.168,pqi:.058,ed:.56},tcoc:{t:1520,a:1580},cost:0,mcd:{ip:195e6,op:120e6,supp:{dsh:58e6,dir:22e6},bn:35000,hedis:{pre:.74,ed:.62,dia:.70,fu:.58}}},
  {id:"210022",nm:"Suburban Hospital",st:"MD",co:1,ty:"ACH",beds:228,tch:"NONE",sn:false,cah:false,wi:1.071,ms:.146,bn:9500,dp:.08,cdi:22,hcc:1.08,bl:{ip:106e6,op:90e6,uc:2.7e6},q:{vbp:1.008,hrrp:.9965,hacrp:1.0,ra:.138,pqi:.038,ed:.44},tcoc:{t:1050,a:1020},cost:0,mcd:{ip:28e6,op:22e6,supp:{dsh:3e6,dir:0},bn:4500,hedis:{pre:.88,ed:.80,dia:.85,fu:.76}}},
  {id:"471001",nm:"Grace Cottage",st:"VT",co:2,ty:"CAH",beds:19,tch:"NONE",sn:false,cah:true,wi:.9685,ms:.224,bn:850,dp:.20,cdi:52,hcc:1.02,bl:{ip:4.1e6,op:4.6e6,uc:.38e6},q:{vbp:.998,hrrp:1.0,hacrp:1.0,ra:.165,pqi:.055,ed:.58},tcoc:{t:980,a:1010},cost:8.6e6,mcd:{ip:1.8e6,op:2.1e6,supp:{dsh:.5e6,dir:0},bn:600,hedis:{pre:.79,ed:.70,dia:.75,fu:.65}}},
  {id:"330059",nm:"Montefiore",st:"NY",co:3,ty:"ACH",beds:1491,tch:"MAJOR",sn:true,cah:false,wi:1.3652,ms:.108,bn:52000,dp:.52,cdi:88,hcc:1.62,bl:{ip:845e6,op:555e6,uc:49e6},q:{vbp:.975,hrrp:.9785,hacrp:.99,ra:.182,pqi:.068,ed:.62},tcoc:{t:1620,a:1720},cost:0,mcd:{ip:520e6,op:340e6,supp:{dsh:85e6,dir:45e6},bn:78000,hedis:{pre:.71,ed:.58,dia:.65,fu:.54}}},
  {id:"070022",nm:"Yale New Haven",st:"CT",co:2,ty:"ACH",beds:1541,tch:"MAJOR",sn:true,cah:false,wi:1.2318,ms:.25,bn:45000,dp:.24,cdi:58,hcc:1.35,bl:{ip:998e6,op:632e6,uc:34e6},q:{vbp:1.012,hrrp:.9908,hacrp:1.0,ra:.128,pqi:.035,ed:.43},tcoc:{t:1320,a:1290},cost:0,mcd:{ip:380e6,op:240e6,supp:{dsh:52e6,dir:18e6},bn:48000,hedis:{pre:.84,ed:.76,dia:.80,fu:.72}}},
  {id:"120001",nm:"Queen's Medical",st:"HI",co:2,ty:"ACH",beds:575,tch:"MAJOR",sn:false,cah:false,wi:1.214,ms:.253,bn:24000,dp:.14,cdi:38,hcc:1.10,bl:{ip:335e6,op:224e6,uc:10.6e6},q:{vbp:1.006,hrrp:.9928,hacrp:1.0,ra:.135,pqi:.038,ed:.46},tcoc:{t:1150,a:1120},cost:0,mcd:{ip:120e6,op:85e6,supp:{dsh:15e6,dir:0},bn:22000,hedis:{pre:.86,ed:.78,dia:.82,fu:.74}}},
  {id:"410007",nm:"Rhode Island Hosp",st:"RI",co:3,ty:"ACH",beds:713,tch:"MAJOR",sn:true,cah:false,wi:1.1425,ms:.265,bn:18000,dp:.28,cdi:62,hcc:1.32,bl:{ip:374e6,op:250e6,uc:17e6},q:{vbp:.994,hrrp:.9868,hacrp:.99,ra:.162,pqi:.054,ed:.55},tcoc:{t:1300,a:1340},cost:0,mcd:{ip:145e6,op:95e6,supp:{dsh:28e6,dir:10e6},bn:20000,hedis:{pre:.77,ed:.68,dia:.73,fu:.62}}},
  {id:"070038",nm:"CT Children's",st:"CT",co:2,ty:"ACH",beds:187,tch:"MAJOR",sn:false,cah:false,wi:1.2318,ms:.31,bn:12000,dp:.18,cdi:35,hcc:.95,bl:{ip:165e6,op:142e6,uc:8e6},q:{vbp:1.022,hrrp:.9975,hacrp:1.0,ra:.095,pqi:.022,ed:.38},tcoc:{t:1080,a:1040},cost:0,mcd:{ip:210e6,op:168e6,supp:{dsh:18e6,dir:8e6},bn:65000,hedis:{pre:.90,ed:.82,dia:.88,fu:.80}}},
  {id:"410012",nm:"Westerly Hosp",st:"RI",co:3,ty:"ACH",beds:84,tch:"NONE",sn:false,cah:false,wi:1.1425,ms:.19,bn:3200,dp:.16,cdi:42,hcc:1.05,bl:{ip:32e6,op:28e6,uc:1.5e6},q:{vbp:1.004,hrrp:.9952,hacrp:1.0,ra:.142,pqi:.045,ed:.48},tcoc:{t:1020,a:1000},cost:0,mcd:{ip:12e6,op:9.5e6,supp:{dsh:2.5e6,dir:0},bn:2800,hedis:{pre:.80,ed:.74,dia:.78,fu:.70}}},
  {id:"330201",nm:"Bellevue",st:"NY",co:3,ty:"ACH",beds:828,tch:"MAJOR",sn:true,cah:false,wi:1.3652,ms:.06,bn:22000,dp:.58,cdi:92,hcc:1.68,bl:{ip:385e6,op:275e6,uc:42e6},q:{vbp:.968,hrrp:.9762,hacrp:.99,ra:.195,pqi:.075,ed:.68},tcoc:{t:1680,a:1820},cost:0,mcd:{ip:445e6,op:310e6,supp:{dsh:110e6,dir:55e6},bn:88000,hedis:{pre:.66,ed:.52,dia:.60,fu:.48}}},
  {id:"210048",nm:"MedStar Good Sam",st:"MD",co:1,ty:"ACH",beds:302,tch:"MINOR",sn:false,cah:false,wi:1.0396,ms:.12,bn:11000,dp:.25,cdi:48,hcc:1.22,bl:{ip:145e6,op:112e6,uc:5.5e6},q:{vbp:1.005,hrrp:.9912,hacrp:1.0,ra:.148,pqi:.042,ed:.46},tcoc:{t:1180,a:1160},cost:0,mcd:{ip:52e6,op:38e6,supp:{dsh:8e6,dir:3e6},bn:8500,hedis:{pre:.81,ed:.73,dia:.77,fu:.69}}},
];
const COH:Record<number,{py1:number;by:number[]}>={1:{py1:2026,by:[2023,2024,2025]},2:{py1:2028,by:[2025,2026,2027]},3:{py1:2028,by:[2025,2026,2027]}};
const APA:Record<number,{ip:number;op:number;uc:number}>={2026:{ip:1.028,op:1.031,uc:1.025},2027:{ip:1.030,op:1.032,uc:1.025},2028:{ip:1.029,op:1.030,uc:1.025},2029:{ip:1.028,op:1.029,uc:1.025},2030:{ip:1.027,op:1.028,uc:1.025}};
const TMD:Record<string,{dsh:number;ime:number}>={NONE:{dsh:.5,ime:0},MINOR:{dsh:1,ime:.7},MAJOR:{dsh:1.5,ime:1.5}};
void TMD;


// ═══════════════════════════════════════════════════════════════════
// MEDICARE HGB ENGINE
// ═══════════════════════════════════════════════════════════════════

function calcMcr(h:AheadHospital,py:number,vA=0):McrResult{
  const co=COH[h.co],cY=co.py1+py-1,bl=h.bl,b3=co.by[2];
  let ic1=1,oc1=1,uc1=1,ic2=1,oc2=1,uc2=1;
  for(let y=co.by[0]+1;y<=b3;y++){const f=APA[y]||APA[2030];ic1*=f.ip;oc1*=f.op;uc1*=f.uc;}
  for(let y=co.by[1]+1;y<=b3;y++){const f=APA[y]||APA[2030];ic2*=f.ip;oc2*=f.op;uc2*=f.uc;}
  const wI=bl.ip*.1*ic1+bl.ip*1.02*.3*ic2+bl.ip*1.04*.6,wO=bl.op*.1*oc1+bl.op*1.02*.3*oc2+bl.op*1.04*.6,wU=bl.uc*.1*uc1+bl.uc*1.01*.3*uc2+bl.uc*1.02*.6;
  const wT=wI+wO+wU;const seed=h.id.split("").reduce((a,c)=>a+c.charCodeAt(0),0);const opt=.998+(seed%10)*.001;
  let iA=1,oA=1,uA=1;for(let y=b3+1;y<=cY;y++){const f=APA[y]||APA[2030];iA*=f.ip;oA*=f.op;uA*=f.uc;}
  const cmi=Math.min(1.05,Math.max(.95,1+(h.hcc-1)*.03*(cY-b3)));const wia=1+(h.wi-1)*.05;
  const pI=wI*opt*iA*cmi*wia,pO=wO*opt*oA*wia,pU=wU*opt*uA;const pA=pI+pO+pU;
  const msa=(1+vA*.5)*1.01,dem=1.006,out=wT*opt*.03;const pV=pA*msa*dem+out;
  const srs=(h.cdi*.7+h.dp*100*.3)/100;const srP=srs>.3?Math.min(.02,srs*.025):0;const sra=pV*srP;
  const qB=h.q.vbp*h.q.hrrp*h.q.hacrp;const qD=(qB-1)*pV;const qF=h.cah?Math.max(0,qD):qD;
  const pau=(1-h.q.ra/.2)*.4+(1-h.q.pqi/.08)*.3+(1-h.q.ed/.65)*.3;const eff=(pau-.5)*.03*(1+srs*.5)*pV;
  const tP=py>=4?(h.tcoc.a-h.tcoc.t)/h.tcoc.t:0;const tA=py>=4?Math.max(-.05,Math.min(.02,Math.abs(tP)>.02?(tP>0?-1:1)*(Math.abs(tP)-.02)*.5:0))*pV:0;
  const hb=py>=4&&h.sn?pV*.004:0;
  let fin=pV+sra+qF+eff+tA+hb;let cOn=false;
  if(h.cah&&h.cost>0){const cahF=h.cost*1.01*Math.pow(1.035,cY-b3);if(fin<cahF){fin=cahF;cOn=true;}}
  const ffs=wT*Math.pow(1.035,cY-b3);
  const wf:WaterfallStep[]=[{n:"Base",v:wT,c:wT},{n:"APA",v:pA-wT*opt,c:pA},{n:"Vol",v:pV-pA,c:pV},{n:"SRA",v:sra,c:0},{n:"Qual",v:qF,c:0},{n:"Eff",v:eff,c:0},{n:"Final",v:fin,c:fin}];
  let cum=wf[0].v;wf.forEach((d,i)=>{if(i>0&&i<wf.length-1){d.c=cum;cum+=d.v;}});
  return{py,cY,wT,wI,wO,wU,opt,pI,pO,pU,pA,pV,sra,qD:qF,qB,eff,tA,hb,fin,ffs,delta:fin-ffs,pct:(fin-ffs)/ffs,cOn,wf};
}

// ═══════════════════════════════════════════════════════════════════
// MEDICAID HGB ENGINE
// ═══════════════════════════════════════════════════════════════════

function calcMcd(h:AheadHospital,py:number,vA=0,mV:number|null=null):McdResult{
  const co=COH[h.co],cY=co.py1+py-1,b3=co.by[2],s=spGet(h.st),m=h.mcd;
  const wI=m.ip*.1+m.ip*1.02*.3+m.ip*1.04*.6,wO=m.op*.1+m.op*1.02*.3+m.op*1.04*.6;const wB=wI+wO;
  const sB=m.supp.dsh+m.supp.dir;
  let iT=1,oT=1;for(let y=b3+1;y<=cY;y++){iT*=s.ipT;oT*=s.opT;}
  const tI=wI*iT,tO=wO*oT;const pT=tI+tO;
  const vF=(1+(mV!==null?mV:vA)*.8)*1.008;const pV=pT*vF;
  const sT=sB*Math.pow(1.025,cY-b3);
  const sdS=(h.cdi*.5+h.dp*100*.3+((1-m.hedis.ed)*100)*.2)/100;
  const sdP=sdS>.25?Math.min(.035,sdS*.04):0;const sdA=pV*sdP;
  const hC=(m.hedis.pre+m.hedis.ed+m.hedis.dia+m.hedis.fu)/4;
  const hA=(hC-.72)*.08*pV;const mcS=pV*s.mcPct*.015;
  const uC=wB*s.upl*Math.pow(1.04,cY-b3);
  let fin=pV+sT+sdA+hA-mcS;let uOn=false;if(fin>uC){fin=uC;uOn=true;}
  const ffs=(wB+sB)*Math.pow(1.04,cY-b3);
  const fSh=fin*s.fmap,stS=fin*(1-s.fmap);
  const wf:WaterfallStep[]=[{n:"Base",v:wB+sB,c:wB+sB},{n:"Trend",v:pT-wB,c:0},{n:"Enr",v:pV-pT,c:0},{n:"Supp",v:sT-sB,c:0},{n:"SDOH",v:sdA,c:0},{n:"HEDIS",v:hA,c:0},{n:"Final",v:fin,c:fin}];
  let cm2=wf[0].v;wf.forEach((d,i)=>{if(i>0&&i<wf.length-1){d.c=cm2;cm2+=d.v;}});
  return{py,cY,wB,wI,wO,sB,wT:wB+sB,tI,tO,pT,pV,sT,sdA,hC,hA,mcS,fin,ffs,delta:fin-ffs,pct:(fin-ffs)/ffs,uC,uOn,fSh,stS,wf};
}

function pjMcr(h:AheadHospital,y:number,v=0){const r:McrResult[]=[];for(let p=1;p<=y;p++)r.push(calcMcr(p===1?h:{...h,bl:{ip:r[p-2].pI/r[p-2].opt,op:r[p-2].pO/r[p-2].opt,uc:r[p-2].pU/r[p-2].opt}},p,v));return r;}
function pjMcd(h:AheadHospital,y:number,v=0,mv:number|null=null){const r:McdResult[]=[];for(let p=1;p<=y;p++)r.push(calcMcd(p===1?h:{...h,mcd:{...h.mcd,ip:r[p-2].tI,op:r[p-2].tO}},p,v,mv));return r;}

// ═══════════════════════════════════════════════════════════════════
// MONTE CARLO
// ═══════════════════════════════════════════════════════════════════

function seeded(s:number){let x=s;return()=>{x=(x*16807)%2147483647;return(x-1)/2147483646;};}
function runMC(h:AheadHospital,yrs:number,vA:number,n=200):McYearResult[]{
  const rng=seeded(h.id.split("").reduce((a,c)=>a+c.charCodeAt(0),0)+42);
  const g=()=>{const u=rng(),v=rng();return Math.sqrt(-2*Math.log(u))*Math.cos(2*Math.PI*v);};
  const all:{m:McrResult;d:McdResult;c:number;mD:number;dD:number;cD:number;cahB:boolean;uplB:boolean}[][]=[];
  for(let s=0;s<n;s++){const aS=1+g()*.008,vS=vA+g()*.03,mS=g()*.012;const sr:{m:McrResult;d:McdResult;c:number;mD:number;dD:number;cD:number;cahB:boolean;uplB:boolean}[]=[];
    for(let p=1;p<=yrs;p++){const bh=p===1?h:{...h,bl:{ip:sr[p-2].m.pI/sr[p-2].m.opt,op:sr[p-2].m.pO/sr[p-2].m.opt,uc:sr[p-2].m.pU/sr[p-2].m.opt}};
      const mr=calcMcr(bh,p,vS),dr=calcMcd(bh,p,vS);const mf=mr.fin*aS,df=dr.fin*(1+mS);
      sr.push({m:{...mr,fin:mf},d:{...dr,fin:df},c:mf+df,mD:mf-mr.ffs,dD:df-dr.ffs,cD:mf+df-mr.ffs-dr.ffs,cahB:mr.cOn,uplB:dr.uOn});}
    all.push(sr);}
  const vr=(v:number[])=>{const m=v.reduce((a,x)=>a+x,0)/v.length;return v.reduce((a,x)=>a+(x-m)**2,0)/v.length;};
  const corr=(a:number[],b:number[])=>{const n2=a.length,ma=a.reduce((s,x)=>s+x,0)/n2,mb=b.reduce((s,x)=>s+x,0)/n2;const sa=Math.sqrt(vr(a)),sb=Math.sqrt(vr(b));if(!sa||!sb)return 0;return a.reduce((s,x,i)=>s+(x-ma)*(b[i]-mb),0)/(n2*sa*sb);};
  const st:McYearResult[]=[];const pc=(x:number)=>Math.min(n-1,Math.floor(n*x));
  for(let p=0;p<yrs;p++){
    const cv=all.map(s=>s[p].c).sort((a,b)=>a-b);const cd=all.map(s=>s[p].cD).sort((a,b)=>a-b);
    const md=all.map(s=>s[p].mD);const dd=all.map(s=>s[p].dD);
    const cf=all[0][p].m.ffs+all[0][p].d.ffs;
    const mV=vr(md),dV=vr(dd),tV=vr(all.map(s=>s[p].cD));
    const var5=cd[pc(.05)];const cvar5=cd.slice(0,Math.max(1,pc(.05)+1)).reduce((a,x)=>a+x,0)/Math.max(1,pc(.05)+1);
    const rho=corr(md,dd);
    st.push({py:p+1,cY:all[0][p].m.cY,cp10:cv[pc(.1)],cp25:cv[pc(.25)],cp50:cv[pc(.5)],cp75:cv[pc(.75)],cp90:cv[pc(.9)],cffs:cf,
      pA:cv.filter(v=>v>cf).length/n,spread:cv[pc(.9)]-cv[pc(.1)],
      mVP:tV>0?mV/tV:.5,dVP:tV>0?dV/tV:.5,var5,cvar5,rho,
      pCAH:all.filter(s=>s[p].cahB).length/n,pUPL:all.filter(s=>s[p].uplB).length/n,
      rawC:cd,rawM:md,rawD:dd});}
  return st;
}

// ═══════════════════════════════════════════════════════════════════
// SENSITIVITY + BREAKEVEN + YOY
// ═══════════════════════════════════════════════════════════════════

function runSens(h:AheadHospital,bD:number,mV:number|null){
  const base=calcMcr(h,1,bD/100).fin+calcMcd(h,1,bD/100,mV!=null?mV/100:null).fin;
  const ev=(hh:AheadHospital,bd=bD,mv=mV)=>calcMcr(hh,1,bd/100).fin+calcMcd(hh,1,bd/100,mv!=null?mv/100:null).fin-base;
  return[
    {nm:"VBP",lo:ev({...h,q:{...h.q,vbp:h.q.vbp-.02}}),hi:ev({...h,q:{...h.q,vbp:h.q.vbp+.02}}),p:"MCR"},
    {nm:"HRRP",lo:ev({...h,q:{...h.q,hrrp:Math.max(.97,h.q.hrrp-.01)}}),hi:ev({...h,q:{...h.q,hrrp:Math.min(1,h.q.hrrp+.005)}}),p:"MCR"},
    {nm:"Volume",lo:ev(h,bD-5),hi:ev(h,bD+5),p:"MCR"},
    {nm:"Enrollment",lo:ev(h,bD,(mV||0)-8),hi:ev(h,bD,(mV||0)+8),p:"MCD"},
    {nm:"HEDIS",lo:ev({...h,mcd:{...h.mcd,hedis:{pre:Math.max(.5,h.mcd.hedis.pre-.08),ed:Math.max(.5,h.mcd.hedis.ed-.08),dia:Math.max(.5,h.mcd.hedis.dia-.08),fu:Math.max(.5,h.mcd.hedis.fu-.08)}}}),hi:ev({...h,mcd:{...h.mcd,hedis:{pre:Math.min(.95,h.mcd.hedis.pre+.08),ed:Math.min(.95,h.mcd.hedis.ed+.08),dia:Math.min(.95,h.mcd.hedis.dia+.08),fu:Math.min(.95,h.mcd.hedis.fu+.08)}}}),p:"MCD"},
    {nm:"DSH",lo:ev({...h,mcd:{...h.mcd,supp:{...h.mcd.supp,dsh:h.mcd.supp.dsh*.8}}}),hi:ev({...h,mcd:{...h.mcd,supp:{...h.mcd.supp,dsh:h.mcd.supp.dsh*1.2}}}),p:"MCD"},
    {nm:"IP Rev",lo:ev({...h,bl:{...h.bl,ip:h.bl.ip*.95}}),hi:ev({...h,bl:{...h.bl,ip:h.bl.ip*1.05}}),p:"MCR"},
    {nm:"Wage Idx",lo:ev({...h,wi:h.wi-.05}),hi:ev({...h,wi:h.wi+.05}),p:"MCR"},
    {nm:"HCC",lo:ev({...h,hcc:h.hcc-.15}),hi:ev({...h,hcc:h.hcc+.15}),p:"MCR"},
    {nm:"Dual%",lo:ev({...h,dp:Math.max(0,h.dp-.1)}),hi:ev({...h,dp:Math.min(1,h.dp+.1)}),p:"Both"},
    {nm:"HACRP",lo:ev({...h,q:{...h.q,hacrp:Math.max(.97,h.q.hacrp-.01)}}),hi:ev({...h,q:{...h.q,hacrp:1.0}}),p:"MCR"},
    {nm:"Dir Pmts",lo:ev({...h,mcd:{...h.mcd,supp:{...h.mcd.supp,dir:h.mcd.supp.dir*.5}}}),hi:ev({...h,mcd:{...h.mcd,supp:{...h.mcd.supp,dir:h.mcd.supp.dir*1.5}}}),p:"MCD"},
  ].sort((a,b)=>Math.max(Math.abs(b.lo),Math.abs(b.hi))-Math.max(Math.abs(a.lo),Math.abs(a.hi)));
}

function runSensTS(h:AheadHospital,yrs:number,bD:number,mV:number|null){
  const vars=["VBP","HRRP","Volume","Enrollment","HEDIS","HCC","DSH","Dual%"];
  const vCl:Record<string,string>={"VBP":cB,"HRRP":"#6366F1","Volume":cG,"Enrollment":cM,"HEDIS":cT,"HCC":cO,"DSH":"#8B5CF6","Dual%":"#EC4899"};
  const pyData:Record<string,number>[]=[];
  for(let py=1;py<=yrs;py++){
    const base=calcMcr(h,py,bD/100).fin+calcMcd(h,py,bD/100,mV!=null?mV/100:null).fin;
    const ev=(hh:AheadHospital,bd=bD,mv=mV)=>calcMcr(hh,py,bd/100).fin+calcMcd(hh,py,bd/100,mv!=null?mv/100:null).fin-base;
    const row:Record<string,number>={py,cY:COH[h.co].py1+py-1,
      VBP:Math.abs(ev({...h,q:{...h.q,vbp:h.q.vbp+.02}}))+Math.abs(ev({...h,q:{...h.q,vbp:h.q.vbp-.02}})),
      HRRP:Math.abs(ev({...h,q:{...h.q,hrrp:Math.min(1,h.q.hrrp+.005)}}))+Math.abs(ev({...h,q:{...h.q,hrrp:Math.max(.97,h.q.hrrp-.01)}})),
      Volume:Math.abs(ev(h,bD+5))+Math.abs(ev(h,bD-5)),
      Enrollment:Math.abs(ev(h,bD,(mV||0)+8))+Math.abs(ev(h,bD,(mV||0)-8)),
      HEDIS:Math.abs(ev({...h,mcd:{...h.mcd,hedis:{pre:Math.min(.95,h.mcd.hedis.pre+.08),ed:Math.min(.95,h.mcd.hedis.ed+.08),dia:Math.min(.95,h.mcd.hedis.dia+.08),fu:Math.min(.95,h.mcd.hedis.fu+.08)}}}))+Math.abs(ev({...h,mcd:{...h.mcd,hedis:{pre:Math.max(.5,h.mcd.hedis.pre-.08),ed:Math.max(.5,h.mcd.hedis.ed-.08),dia:Math.max(.5,h.mcd.hedis.dia-.08),fu:Math.max(.5,h.mcd.hedis.fu-.08)}}})),
      HCC:Math.abs(ev({...h,hcc:h.hcc+.15}))+Math.abs(ev({...h,hcc:h.hcc-.15})),
      DSH:Math.abs(ev({...h,mcd:{...h.mcd,supp:{...h.mcd.supp,dsh:h.mcd.supp.dsh*1.2}}}))+Math.abs(ev({...h,mcd:{...h.mcd,supp:{...h.mcd.supp,dsh:h.mcd.supp.dsh*.8}}})),
      "Dual%":Math.abs(ev({...h,dp:Math.min(1,h.dp+.1)}))+Math.abs(ev({...h,dp:Math.max(0,h.dp-.1)})),
    };pyData.push(row);
  }
  const shifts=vars.map(v=>{const first=pyData[0]?.[v]||0,last=pyData[pyData.length-1]?.[v]||0;const chg=first>0?(last-first)/first:0;return{v,first,last,chg,dir:chg>.15?"RISING" as const:chg<-.15?"FALLING" as const:"STABLE" as const,cl:vCl[v]};}).sort((a,b)=>Math.abs(b.chg)-Math.abs(a.chg));
  return{pyData,shifts,vars,vCl};
}
function solveBE(h:AheadHospital,bD:number,mV:number|null){
  const ffs=calcMcr(h,1,bD/100).ffs+calcMcd(h,1,bD/100,mV!=null?mV/100:null).ffs;
  const cb=(hh:AheadHospital,bd:number,mv:number|null)=>calcMcr(hh,1,bd/100).fin+calcMcd(hh,1,bd/100,mv!=null?mv/100:null).fin;
  const bs=(fn:(x:number)=>number,lo:number,hi:number)=>{for(let i=0;i<40;i++){const m=(lo+hi)/2;fn(m)>ffs?hi=m:lo=m;}return(lo+hi)/2;};
  return[
    (()=>{const c=h.q.vbp,v=bs(x=>cb({...h,q:{...h.q,vbp:x}},bD,mV),.95,1.05);return{nm:"VBP",cur:c.toFixed(3),be:v.toFixed(3),gap:((v-c)*1000).toFixed(1)+"pt",ok:v<=1.04&&v>=.96,p:"MCR"};})(),
    (()=>{const c=h.q.hrrp,v=bs(x=>cb({...h,q:{...h.q,hrrp:x}},bD,mV),.96,1);return{nm:"HRRP",cur:c.toFixed(4),be:v.toFixed(4),gap:((v-c)*1e4).toFixed(0)+"bp",ok:v<=1&&v>=.96,p:"MCR"};})(),
    (()=>{const v=bs(x=>cb(h,x,mV),-15,15);return{nm:"Vol",cur:bD+"%",be:v.toFixed(1)+"%",gap:(v-bD).toFixed(1)+"pp",ok:Math.abs(v)<15,p:"MCR"};})(),
    (()=>{const hd=h.mcd.hedis,c=(hd.pre+hd.ed+hd.dia+hd.fu)/4,v=bs(x=>cb({...h,mcd:{...h.mcd,hedis:{pre:x,ed:x,dia:x,fu:x}}},bD,mV),.5,.95);return{nm:"HEDIS",cur:(c*100).toFixed(0)+"th",be:(v*100).toFixed(0)+"th",gap:((v-c)*100).toFixed(1)+"pp",ok:v<=.95,p:"MCD"};})(),
  ];
}
function decompYoY(mp:McrResult[],dp:McdResult[]){const r:{cY:number;apaD:number;volD:number;qualD:number;mcdT:number;mcdE:number;mcdH:number;cT:number}[]=[];for(let i=1;i<mp.length;i++){const pm=mp[i-1],cm=mp[i],pd=dp[i-1],cd=dp[i];r.push({cY:cm.cY,apaD:cm.pA-pm.pA,volD:(cm.pV-cm.pA)-(pm.pV-pm.pA),qualD:(cm.qD-pm.qD)+(cm.eff-pm.eff),mcdT:cd.pT-pd.pT,mcdE:(cd.pV-cd.pT)-(pd.pV-pd.pT),mcdH:cd.hA-pd.hA,cT:cm.fin-pm.fin+cd.fin-pd.fin});}return r;}

// ═══════════════════════════════════════════════════════════════════
// v7-v9 ENGINES
// ═══════════════════════════════════════════════════════════════════

function calcPortfolio(hs:AheadHospital[]){return hs.map(h=>{const mc=runMC(h,5,0,80);const mr=calcMcr(h,1),dr=calcMcd(h,1);const ret=((mr.fin+dr.fin)-(mr.ffs+dr.ffs))/(mr.ffs+dr.ffs)*100;const risk=mc[0]?mc[0].spread/(mr.ffs+dr.ffs)*100:5;return{nm:h.nm.length>14?h.nm.slice(0,14):h.nm,id:h.id,ret,risk,sharpe:risk>0?ret/risk:0,st:h.st};});}
function svcLine(h:AheadHospital){const mr=calcMcr(h,1),dr=calcMcd(h,1);return[{nm:"MCR IP",v:mr.pI-h.bl.ip*1.035,c:cB},{nm:"MCR OP",v:mr.pO-h.bl.op*1.035,c:`${cB}AA`},{nm:"MCR Adj",v:mr.sra+mr.qD+mr.eff+mr.tA+mr.hb,c:"#48639c"},{nm:"MCD IP",v:dr.tI*1.008-dr.wI*1.04,c:cM},{nm:"MCD OP",v:dr.tO*1.008-dr.wO*1.04,c:`${cM}AA`},{nm:"MCD Supp",v:dr.sT-(dr.sB||0)*1.04,c:`${cM}66`},{nm:"MCD Adj",v:dr.sdA+dr.hA-dr.mcS,c:cP}].sort((a,b)=>Math.abs(b.v)-Math.abs(a.v));}
function cohortTime(h:AheadHospital){return[1,2,3].map(co=>{const hc={...h,co};const mr=pjMcr(hc,5),dr=pjMcd(hc,5);return{co,py1:COH[co].py1,res:mr.map((m,i)=>({py:m.py,d:m.delta+dr[i].delta,pct:(m.fin+dr[i].fin-m.ffs-dr[i].ffs)/(m.ffs+dr[i].ffs)}))};});}
function calcCred(h:AheadHospital,mp:McrResult[],dp:McdResult[],mc:McYearResult[]){const n=h.bn;const peers=H.filter(x=>x.tch===h.tch&&x.id!==h.id);if(peers.length<2)return{z:1,adj:mp.map((m,i)=>({py:m.py,cY:m.cY,raw:m.delta+dp[i].delta,cred:m.delta+dp[i].delta,peer:0})),peers:0,k:0};const pD=peers.map(p=>{const r=calcMcr(p,1),d=calcMcd(p,1);return r.delta+d.delta;});const pM=pD.reduce((s,x)=>s+x,0)/pD.length;const pV=pD.reduce((s,x)=>s+(x-pM)**2,0)/pD.length;const iV=mc[0]?mc[0].spread**2/16:pV;const k=pV>0?iV/pV*1000:5000;const z=Math.min(1,n/(n+k));return{z,adj:mp.map((m,i)=>{const raw=m.delta+dp[i].delta;const sc=pM*Math.pow(1.02,i);return{py:m.py,cY:m.cY,raw,cred:z*raw+(1-z)*sc,peer:sc};}),peers:peers.length,k:Math.round(k)};}

function calcNash(hs:AheadHospital[]){const mkts:Record<string,AheadHospital[]>={};hs.forEach(h=>{if(!mkts[h.st])mkts[h.st]=[];mkts[h.st].push(h);});const res:{nm:string;st:string;alone:number;withAll:number;nash:boolean;eq:string;cn:number}[]=[];
  Object.entries(mkts).forEach(([st,hss])=>{if(hss.length<2){hss.forEach(h=>{const mr=calcMcr(h,1),dr=calcMcd(h,1);res.push({nm:h.nm,st,alone:mr.delta+dr.delta,withAll:mr.delta+dr.delta,nash:true,eq:"Dominant",cn:0});});return;}
    hss.forEach(h=>{const mr0=calcMcr(h,1),dr0=calcMcd(h,1);const alone=mr0.delta+dr0.delta;const cMS=hss.filter(x=>x.id!==h.id).reduce((s,x)=>s+x.ms,0);const cn=cMS*.15;const mrC=calcMcr(h,1,-cn),drC=calcMcd(h,1,-cn);const wa=mrC.delta+drC.delta;const nash=wa>0;const eq=alone>0&&wa>0?"Dominant":alone>0&&wa<=0?"Contingent":alone<=0?"Avoid":"Contrarian";res.push({nm:h.nm,st,alone,withAll:wa,nash,eq,cn:cn*100});});});
  return{results:res,stable:res.filter(r=>r.nash).length,total:res.length};}

function calcOpts(h:AheadHospital,mc:McYearResult[]){const mp=pjMcr(h,5),dp=pjMcd(h,5);const disc=.04;const npv=mp.reduce((s,m,i)=>s+(m.delta+dp[i].delta)/Math.pow(1+disc,i+1),0);
  const npvC=(co:number)=>{const hc={...h,co};const m=pjMcr(hc,5),d=pjMcd(hc,5);const dly=COH[co].py1-2026;return m.reduce((s,r,i)=>s+(r.delta+d[i].delta)/Math.pow(1+disc,i+1+dly),0);};
  const npvs=[npvC(1),npvC(2),npvC(3)];const timeV=Math.max(0,Math.max(npvs[1],npvs[2])-npvs[0]);
  const sigma=mc[0]?mc[0].spread/(calcMcr(h,1).ffs+calcMcd(h,1).ffs):.1;const stratV=npv>0?Math.abs(npv)*.08:.02*Math.abs(npv);const learnV=sigma*.5*Math.abs(npv);
  const optVal=npv+timeV+stratV+learnV;const exNow=npv+stratV>timeV+learnV;
  return{npv,timeV,stratV,learnV,sigma,optVal,exNow,npvs,rec:exNow?"Participate Now":"Defer",recCl:exNow?C.pos:C.warn};}

function calcContract(h:AheadHospital,bD:number,mV:number|null){const mr=calcMcr(h,1,bD/100),dr=calcMcd(h,1,bD/100,mV!=null?mV/100:null);const gap=mr.ffs+dr.ffs-mr.fin-dr.fin;const curAPA=APA[COH[h.co].py1]||APA[2030];const apaG=gap>0?gap/(mr.wT+dr.wT):0;
  const pctDev=Math.abs(mr.fin+dr.fin-mr.ffs-dr.ffs)/(mr.ffs+dr.ffs);const cW=Math.max(.02,pctDev*1.5);
  return{terms:[{nm:"APA Floor",cur:((curAPA.ip-1)*100).toFixed(1)+"%",need:((curAPA.ip+apaG*.6-1)*100).toFixed(1)+"%",ok:apaG<.005,imp:gap*.6,p:"MCR"},
    {nm:"Risk Corridor",cur:"None",need:`±${(cW*100).toFixed(1)}%`,ok:cW<.05,imp:gap*.4,p:"Both"},
    {nm:"VBP Floor",cur:"None",need:Math.min(h.q.vbp,.99).toFixed(3),ok:true,imp:Math.max(0,(1-h.q.vbp)*mr.pV*.5),p:"MCR"},
    {nm:"Savings Split",cur:"100%",need:`${(Math.min(.8,gap>0?gap/Math.abs(mr.fin-mr.ffs+.01):.5)*100).toFixed(0)}%`,ok:gap<mr.wT*.02,imp:gap*.5,p:"Both"},
    {nm:"UPL Multiple",cur:spGet(h.st).upl.toFixed(2)+"×",need:(dr.uOn?spGet(h.st).upl*1.1:spGet(h.st).upl).toFixed(2)+"×",ok:!dr.uOn,imp:dr.uOn?gap*.2:0,p:"MCD"},
  ],gap,viable:gap<=0||3<=5};}

function calcEVPI(h:AheadHospital,mc:McYearResult[],_mr:McrResult,_dr:McdResult){if(!mc[0]?.rawC)return{evpi:0,evsi:[] as {nm:string;val:number;pct:number;inv:string}[],baseEV:0,perfEV:0};const raw=mc[0].rawC;const n=raw.length;const baseEV=raw.reduce((s,x)=>s+x,0)/n;const perfEV=raw.reduce((s,x)=>s+Math.max(0,x),0)/n;const evpi=perfEV-Math.max(0,baseEV);
  const evsi=[{nm:"Volume Forecast",val:evpi*.35,pct:35,inv:"$150K–300K"},{nm:"Quality Scoring",val:evpi*.20,pct:20,inv:"$50K–100K"},{nm:"CDI/HCC Audit",val:evpi*.15,pct:15,inv:"$75K–150K"},{nm:"MCD Enrollment",val:evpi*.15,pct:15,inv:"$100K–200K"},{nm:"APA Trend",val:evpi*.10,pct:10,inv:"$25K–50K"},{nm:"Peer Benchmark",val:evpi*.05,pct:5,inv:"$20K–40K"}].sort((a,b)=>b.val-a.val);
  return{evpi,evsi,baseEV,perfEV};}

function calcRegimes(h:AheadHospital,bD:number,mV:number|null){
  const regs=[{nm:"Status Quo",prob:.55,mod:{} as Record<string,number>},{nm:"Risk Corridors",prob:.15,mod:{corr:.03}},{nm:"APA Tightened",prob:.12,mod:{apaA:-.005}},{nm:"MCD Expansion",prob:.08,mod:{mcdB:.12}},{nm:"Cancelled",prob:.05,mod:{cancel:1}},{nm:"Qual×2",prob:.05,mod:{qm:2}}];
  const res=regs.map(r=>{if(r.mod.cancel)return{...r,delta:0,pct:0,comb:0};
    const hM={...h,q:{...h.q}};if(r.mod.qm){hM.q.vbp=1+(h.q.vbp-1)*r.mod.qm;hM.q.hrrp=1+(h.q.hrrp-1)*r.mod.qm;}
    const mA=mV!=null?mV/100+(r.mod.mcdB||0):r.mod.mcdB||null;const mr=calcMcr(hM,1,bD/100),dr=calcMcd(hM,1,bD/100,mA);let mF=mr.fin,dF=dr.fin;
    if(r.mod.apaA)mF*=(1+r.mod.apaA);if(r.mod.corr){const ffs=mr.ffs+dr.ffs;const act=(mF+dF-ffs)/ffs;if(act<-r.mod.corr)dF+=(Math.abs(act)-r.mod.corr)*ffs*.5;if(act>r.mod.corr)dF-=(act-r.mod.corr)*ffs*.5;}
    return{...r,comb:mF+dF,delta:mF+dF-mr.ffs-dr.ffs,pct:(mF+dF-mr.ffs-dr.ffs)/(mr.ffs+dr.ffs)};});
  const wEV=res.reduce((s,r)=>s+r.prob*r.delta,0);
  return{results:res,wEV,recCl:wEV>0?C.pos:C.neg};}

function calcBayesian(h:AheadHospital,mc:McYearResult[],mp:McrResult[],dp:McdResult[]){
  if(!mc[0]?.rawC)return{years:[] as {py:number;cY:number;priorMu:number;priorSig:number;obs:number;postMu:number;postSig:number;ci95l:number;ci95u:number;ci80l:number;ci80u:number;pPos:number;sigRed:number}[],origSig:0,finalSig:0,totalRed:0};
  const prior=mc[0].rawC;const n=prior.length;
  const priorMu=prior.reduce((s,x)=>s+x,0)/n;
  const priorSig=Math.sqrt(prior.reduce((s,x)=>s+(x-priorMu)**2,0)/n);
  const years:{py:number;cY:number;priorMu:number;priorSig:number;obs:number;postMu:number;postSig:number;ci95l:number;ci95u:number;ci80l:number;ci80u:number;pPos:number;sigRed:number}[]=[];
  let cumMu=priorMu,cumSig=priorSig;
  for(let py=0;py<Math.min(mp.length,5);py++){
    const seed=(h.id.split("").reduce((a,c)=>a+c.charCodeAt(0),0)+py*7)%100;
    const obsNoise=(seed-50)/50*priorSig*.4;
    const observed=mp[py].delta+dp[py].delta+obsNoise;
    const obsSig=priorSig*.6;
    const postPrec=1/(cumSig**2)+1/(obsSig**2);
    const postMu=(cumMu/(cumSig**2)+observed/(obsSig**2))/postPrec;
    const postSig=Math.sqrt(1/postPrec);
    const ci95:[number,number]=[postMu-1.96*postSig,postMu+1.96*postSig];
    const ci80:[number,number]=[postMu-1.28*postSig,postMu+1.28*postSig];
    const pPos=1-normalCDF(-postMu/postSig);
    years.push({py:py+1,cY:mp[py].cY,priorMu:cumMu,priorSig:cumSig,obs:observed,postMu,postSig,ci95l:ci95[0],ci95u:ci95[1],ci80l:ci80[0],ci80u:ci80[1],pPos,sigRed:1-postSig/priorSig});
    cumMu=postMu;cumSig=postSig;
  }
  return{years,origSig:priorSig,finalSig:cumSig,totalRed:1-(cumSig/priorSig)};
}
function normalCDF(x:number){const t=1/(1+.2316419*Math.abs(x));const d=.3989423*Math.exp(-x*x/2);const p=d*t*(.3193815+t*(-.3565638+t*(1.781478+t*(-1.821256+t*1.330274))));return x>0?1-p:p;}

function calcShapley(hs:AheadHospital[]){
  const mkts:Record<string,AheadHospital[]>={};hs.forEach(h=>{if(!mkts[h.st])mkts[h.st]=[];mkts[h.st].push(h);});
  const results:{nm:string;st:string;shapley:number;naive:number;diff:number;pctTot:number}[]=[];
  Object.entries(mkts).forEach(([st,hss])=>{
    if(hss.length<2){hss.forEach(h=>{const mr=calcMcr(h,1),dr=calcMcd(h,1);results.push({nm:h.nm,st,shapley:mr.delta+dr.delta,naive:mr.delta+dr.delta,diff:0,pctTot:100});});return;}
    const coalVal=(subset:AheadHospital[])=>{if(subset.length===0)return 0;
      return subset.reduce((s,h)=>{const ext=subset.filter(x=>x.id!==h.id).reduce((a,x)=>a+x.ms,0);const cn=-ext*.15;const mr=calcMcr(h,1,cn),dr=calcMcd(h,1,cn);return s+mr.delta+dr.delta;},0);};
    const nH=hss.length;const totalCoal=coalVal(hss);
    hss.forEach((h,idx)=>{
      let shapley=0;const others=hss.filter((_,i)=>i!==idx);
      const nO=others.length;const nSub=1<<nO;
      for(let mask=0;mask<nSub;mask++){
        const subset=others.filter((_,j)=>(mask&(1<<j))!==0);
        const vWith=coalVal([...subset,h]);const vWithout=coalVal(subset);
        const marginal=vWith-vWithout;
        const sz=subset.length;let w=1;for(let k=1;k<=sz;k++)w*=k;for(let k=1;k<=nH-sz-1;k++)w*=k;let nFact=1;for(let k=1;k<=nH;k++)nFact*=k;
        shapley+=w*marginal/nFact;
      }
      const naive=calcMcr(h,1).delta+calcMcd(h,1).delta;
      results.push({nm:h.nm,st,shapley,naive,diff:shapley-naive,pctTot:totalCoal!==0?shapley/totalCoal*100:0});
    });
  });
  return results;
}

function calcMarkov(h:AheadHospital){
  const T=[[.45,.30,.18,.07],[.15,.42,.28,.15],[.08,.20,.44,.28],[.05,.12,.25,.58]];
  const vbpQ=h.q.vbp>=1.015?3:h.q.vbp>=1.002?2:h.q.vbp>=.988?1:0;
  const hrrpQ=h.q.hrrp>=.997?3:h.q.hrrp>=.991?2:h.q.hrrp>=.982?1:0;
  const hedisQ=calcMcd(h,1).hC>=.80?3:calcMcd(h,1).hC>=.73?2:calcMcd(h,1).hC>=.65?1:0;
  const trajectory=(startQ:number)=>{let dist=[0,0,0,0];dist[startQ]=1;const years=[{py:0,dist:[...dist]}];
    for(let y=1;y<=5;y++){const next=[0,0,0,0];for(let i=0;i<4;i++)for(let j=0;j<4;j++)next[j]+=dist[i]*T[i][j];years.push({py:y,dist:[...next]});dist=next;}return years;};
  let ss=[.25,.25,.25,.25];for(let i=0;i<50;i++){const n=[0,0,0,0];for(let j=0;j<4;j++)for(let k=0;k<4;k++)n[k]+=ss[j]*T[j][k];const s=n.reduce((a,x)=>a+x,0);ss=n.map(x=>x/s);}
  const mr1=calcMcr(h,1),mr2=calcMcr({...h,q:{...h.q,vbp:Math.min(1.03,h.q.vbp+.01)}},1);
  const qImp=(mr2.fin-mr1.fin)*4;
  return{T,vbpQ,hrrpQ,hedisQ,vbpTraj:trajectory(vbpQ),hrrpTraj:trajectory(hrrpQ),hedisTraj:trajectory(hedisQ),steadyState:ss,qImp,labels:["Q1 (Bottom)","Q2","Q3","Q4 (Top)"]};}

function calcProspect(mc:McYearResult[]){
  if(!mc[0]?.rawC)return{ev:0,pv:0,ratio:1,regretP:0,lossAv:2.25,alpha:.88,years:[] as {py:number;cY:number;ev:number;pv:number;ratio:number;regretP:number}[]};
  const lambda=2.25;const alpha=.88;
  const vFn=(x:number)=>x>=0?Math.pow(x/1e6,alpha):-lambda*Math.pow(-x/1e6,alpha);
  const gamma=.69;const wFn=(p:number)=>{const pg=Math.pow(p,gamma);return pg/Math.pow(pg+Math.pow(1-p,gamma),1/gamma);};
  const years=mc.slice(0,5).map(m=>{
    const raw=m.rawC||[];const n=raw.length;if(!n)return{py:m.py,cY:m.cY,ev:0,pv:0,ratio:1,regretP:0};
    const ev=raw.reduce((s,x)=>s+x,0)/n;
    const sorted=[...raw].sort((a,b)=>a-b);
    let pv=0;for(let i=0;i<n;i++){const w=wFn((i+1)/n)-wFn(i/n);pv+=vFn(sorted[i])*w;}
    const pvScaled=pv>=0?Math.pow(pv,1/alpha)*1e6:-Math.pow(-pv/lambda,1/alpha)*1e6;
    const regretP=raw.filter(x=>x<0).length/n;
    return{py:m.py,cY:m.cY,ev,pv:pvScaled,ratio:ev!==0?pvScaled/ev:1,regretP};
  });
  const py1=years[0]||{ev:0,pv:0,ratio:1,regretP:0};
  return{...py1,lossAv:lambda,alpha,years};
}

function calcCopula(mc:McYearResult[]){
  if(!mc[0]?.rawM||!mc[0]?.rawD)return{years:[] as {py:number;cY:number;lL:number;lU:number;tailRho:number;midRho:number;rho:number;div:string}[]};
  return{years:mc.slice(0,5).map(m=>{
    const mR=m.rawM,dR=m.rawD;const n=mR.length;
    const rank=(arr:number[])=>{const s=[...arr].sort((a,b)=>a-b);return arr.map(v=>s.indexOf(v)/n);};
    const uM=rank(mR),uD=rank(dR);
    const q=.15;const lCount=uM.filter((u,i)=>u<q&&uD[i]<q).length/n;const lambdaL=lCount/q;
    const uCount=uM.filter((u,i)=>u>(1-q)&&uD[i]>(1-q)).length/n;const lambdaU=uCount/q;
    const tailIdx=uM.map((u,i)=>u<.2||u>.8?i:-1).filter(i=>i>=0);const midIdx=uM.map((u,i)=>u>=.3&&u<=.7?i:-1).filter(i=>i>=0);
    const subCorr=(idx:number[])=>{if(idx.length<5)return 0;const mS=idx.map(i=>mR[i]),dS=idx.map(i=>dR[i]);const mm=mS.reduce((s,x)=>s+x,0)/mS.length,dm=dS.reduce((s,x)=>s+x,0)/dS.length;const sv=Math.sqrt(mS.reduce((s,x)=>s+(x-mm)**2,0)/mS.length),dv=Math.sqrt(dS.reduce((s,x)=>s+(x-dm)**2,0)/dS.length);if(!sv||!dv)return 0;return mS.reduce((s,x,i)=>s+(x-mm)*(dS[i]-dm),0)/(mS.length*sv*dv);};
    const tailRho=subCorr(tailIdx),midRho=subCorr(midIdx);
    const divReliable=lambdaL<.3?"HIGH":lambdaL<.5?"MED":"LOW";
    return{py:m.py,cY:m.cY,lL:Math.min(1,lambdaL),lU:Math.min(1,lambdaU),tailRho,midRho,rho:m.rho,div:divReliable};
  })};
}

function calcOptStop(h:AheadHospital,mc:McYearResult[],mp:McrResult[],dp:McdResult[]){
  const disc=.96;const yrs=Math.min(mp.length,5);
  const pyDeltas=mp.map((m,i)=>({py:m.py,cY:m.cY,expD:m.delta+dp[i].delta,p10:mc[i]?mc[i].var5:0,p90:mc[i]?(mc[i].cp90-mc[i].cffs):0}));
  const contVal=new Array(yrs).fill(0) as number[];
  for(let t=yrs-1;t>=0;t--){
    const futV=t<yrs-1?contVal[t+1]:0;
    contVal[t]=pyDeltas[t].expD+disc*futV;
  }
  const frontier:{py:number;cY:number;expD:number;cumD:number;contV:number;futOpt:number;exitThresh:number;shouldCont:boolean;p10D:number}[]=[];let cumD=0;
  for(let t=0;t<yrs;t++){
    cumD+=pyDeltas[t].expD;
    const futOpt=contVal[t]-pyDeltas[t].expD;
    const exitThresh=-futOpt;
    frontier.push({py:t+1,cY:pyDeltas[t].cY,expD:pyDeltas[t].expD,cumD,contV:contVal[t],futOpt,exitThresh,
      shouldCont:contVal[t]>0,p10D:pyDeltas[t].p10});
  }
  const firstExit=frontier.findIndex(f=>!f.shouldCont);
  return{frontier,firstExit:firstExit>=0?firstExit+1:-1,optPolicy:firstExit<0?"Continue all years":`Consider exit after PY${firstExit+1}`,
    totalContV:contVal[0]};
}

// ═══════════════════════════════════════════════════════════════════
// DECISION SCORECARD + SYNTHESIS + INTERVENTIONS + STRESS + APM
// ═══════════════════════════════════════════════════════════════════

const DEF_WT:Record<string,number>={MCR:15,MCD:15,"Multi-Yr":20,Quality:10,HEDIS:10,"P(>FFS)":15,SDOH:15};
function calcSc(h:AheadHospital,mr:McrResult,dr:McdResult,mp:McrResult[],dp:McdResult[],mc:McYearResult[],wts?:Record<string,number>){
  const w=wts||DEF_WT;
  const f:{nm:string;sc:number;wt:number;det:string;cl:string}[]=[];const s=(v:number,t:number[])=>t.reduce((r,x,i)=>v>x?10-i*2:r,0);
  f.push({nm:"MCR",sc:s(mr.pct,[.02,.01,0,-.01,-.02]),wt:w.MCR||15,det:fP(mr.pct),cl:mr.pct>0?C.pos:C.neg});
  f.push({nm:"MCD",sc:s(dr.pct,[.02,.01,0,-.01,-.02]),wt:w.MCD||15,det:fP(dr.pct),cl:dr.pct>0?C.pos:C.neg});
  const cT2=mp.reduce((a,p)=>a+p.delta,0)+dp.reduce((a,p)=>a+p.delta,0);const cS=cT2>0?Math.min(10,6+cT2/(mr.wT+dr.wT)*80):Math.max(0,4+cT2/(mr.wT+dr.wT)*40);
  f.push({nm:"Multi-Yr",sc:Math.round(cS),wt:w["Multi-Yr"]||20,det:fmt(cT2),cl:cS>=6?C.pos:C.neg});
  const qC=h.q.vbp*h.q.hrrp*h.q.hacrp;f.push({nm:"Quality",sc:s(qC,[1.01,1.005,1,.995,.985]),wt:w.Quality||10,det:qC.toFixed(3)+"×",cl:qC>=1?C.pos:C.neg});
  f.push({nm:"HEDIS",sc:s(dr.hC,[.80,.75,.72,.68,.60]),wt:w.HEDIS||10,det:(dr.hC*100).toFixed(0)+"th",cl:dr.hC>=.75?C.pos:C.neg});
  const mcP=mc[0]?.pA||.5;f.push({nm:"P(>FFS)",sc:Math.round(mcP*10),wt:w["P(>FFS)"]||15,det:(mcP*100).toFixed(0)+"%",cl:mcP>=.7?C.pos:C.neg});
  f.push({nm:"SDOH",sc:s((dr.sdA+mr.sra)/(mr.wT+dr.wT),[.012,.008,.004,0]),wt:w.SDOH||15,det:fmt(dr.sdA+mr.sra),cl:C.pos});
  const tot=f.reduce((a,x)=>a+x.sc*x.wt,0)/f.reduce((a,x)=>a+10*x.wt,0)*100;
  return{factors:f,comp:Math.round(tot),rec:tot>=70?"Favorable":tot>=50?"Conditional":"Unfavorable",recCl:tot>=70?C.pos:tot>=50?C.warn:C.neg};
}

function calcSynthesis(h:AheadHospital,mr:McrResult,dr:McdResult,mp:McrResult[],dp:McdResult[],mc:McYearResult[],
  dec:ReturnType<typeof calcSc>,opts:ReturnType<typeof calcOpts>,prospect:ReturnType<typeof calcProspect>,
  bayesian:ReturnType<typeof calcBayesian>,cred:ReturnType<typeof calcCred>,nash:ReturnType<typeof calcNash>,
  evpi:ReturnType<typeof calcEVPI>,regimes:ReturnType<typeof calcRegimes>,contract:ReturnType<typeof calcContract>,
  optStop:ReturnType<typeof calcOptStop>,copula:ReturnType<typeof calcCopula>,markov:ReturnType<typeof calcMarkov>){
  const ffs=mr.ffs+dr.ffs,fin=mr.fin+dr.fin,delta=fin-ffs,pct=delta/ffs;
  const signals:{engine:string;finding:string;dir:string;mat:number;conf:number;action:string}[]=[];
  const add=(engine:string,finding:string,dir:string,mat:number,conf:number,action:string)=>signals.push({engine,finding,dir,mat:Math.abs(mat),conf,action});
  add("HGB",`Combined PY1: ${fmt(delta)} (${fP(pct)})`,delta>0?"FAVOR":"AGAINST",delta,.9,delta>0?"Positive baseline supports participation":"Negative baseline — requires contract modifications or operational investment");
  const pW=mc[0]?.pA||.5;add("Monte Carlo",`${(pW*100).toFixed(0)}% probability of outperforming FFS (200 sims)`,pW>.55?"FAVOR":"AGAINST",delta*pW,.85,pW>.65?"Strong stochastic support":"Significant downside tail risk");
  add("Real Options",`${opts.rec}. NPV ${fmt(opts.npv)}, total option value ${fmt(opts.optVal)}`,opts.exNow?"FAVOR":"DEFER",opts.optVal,.8,opts.exNow?"Exercise now — intrinsic exceeds time value":"Defer — time and learning value exceed intrinsic");
  const ptR=prospect.years?.[0]?.ratio||1;add("Prospect Theory",`PT/EV: ${ptR.toFixed(2)}× — decision ${ptR<.85?"feels significantly worse than math":"aligns with rational expectation"}`,ptR>.85?"NEUTRAL":"CAUTION",delta*(1-ptR),.75,ptR<.85?"Frame board presentation around upside, not EV":"Standard analytical presentation appropriate");
  const bF=bayesian.years?.[bayesian.years.length-1];if(bF)add("Bayesian",`Post-data P(Δ>0) = ${(bF.pPos*100).toFixed(0)}%, σ reduced ${((bayesian.totalRed||0)*100).toFixed(0)}%`,bF.pPos>.6?"FAVOR":"AGAINST",delta*bF.pPos,.7,"Uncertainty resolves with participation — learning value embedded in option price");
  add("Bühlmann",`Z=${cred.z.toFixed(2)} — ${cred.z>.8?"hospital data reliable":cred.z>.5?"moderate peer blend":"heavily peer-weighted"}`,cred.z>.6?"NEUTRAL":"CAUTION",0,.7,cred.z<.5?"Invest in data infrastructure before high-confidence commitment":"Projections well-supported by experience base");
  const myN=nash.results.find(r=>r.nm===h.nm);if(myN)add("Nash",`Market: ${myN.eq}. ${myN.nash?"Stable regardless of competitors":"Depends on competitor decisions"}`,myN.nash?"FAVOR":"CAUTION",myN.withAll,.75,myN.eq==="Dominant"?"Join independently — competitors irrelevant":"Monitor competitor participation signals");
  add("EVPI",`Perfect info worth ${fmt(evpi.evpi)}. Top: ${evpi.evsi[0]?.nm||"N/A"} (${fmt(evpi.evsi[0]?.val||0)})`,evpi.evpi>Math.abs(delta)*.2?"INVESTIGATE":"PROCEED",evpi.evsi[0]?.val||0,.65,evpi.evpi>1e6?`Invest in ${evpi.evsi[0]?.nm} before finalizing`:"Information gains marginal — proceed with current data");
  add("Regimes",`Weighted EV: ${fmt(regimes.wEV)} across 6 political scenarios`,regimes.wEV>0?"FAVOR":"AGAINST",regimes.wEV,.6,"Monitor CMS rulemaking for regime-shifting signals");
  add("Contract",contract.gap<=0?"Terms favorable":"Gap of "+fmt(contract.gap)+" requires term modifications",contract.gap<=0?"FAVOR":"AGAINST",contract.gap,.8,contract.gap>0?`Negotiate ${contract.terms.filter(t=>t.ok).length} achievable modifications`:"Proceed under standard AHEAD terms");
  add("Opt Stopping",optStop.frontier.every(f=>f.shouldCont)?"Continue all years recommended":optStop.optPolicy,optStop.frontier.every(f=>f.shouldCont)?"FAVOR":"CAUTION",optStop.frontier[0]?.contV||0,.7,optStop.frontier.every(f=>f.shouldCont)?"Full commitment appropriate":"Build exit triggers at identified thresholds");
  const c0=copula.years?.[0];if(c0)add("Copula",`Diversification: ${c0.div}. Lower-tail λ=${c0.lL.toFixed(2)}`,c0.div==="HIGH"?"FAVOR":"CAUTION",0,.6,c0.div!=="HIGH"?"Dual-payer hedge unreliable in crises":"Strong diversification from dual-payer structure");
  add("Markov",`Quality quartile jump worth ${fmt(markov.qImp)}. VBP at Q${markov.vbpQ+1}`,markov.vbpQ>=2?"FAVOR":"INVEST",markov.qImp,.5,markov.vbpQ<2?"Quality improvement has outsized ROI — invest before participation":"Quality position supports favorable trajectory");
  signals.sort((a,b)=>b.mat-a.mat);
  const contras:{a:string;b:string;res:string}[]=[];
  const pro=signals.filter(s=>s.dir==="FAVOR"),con=signals.filter(s=>s.dir==="AGAINST");
  if(pro.length&&con.length)contras.push({a:pro[0].engine,b:con[0].engine,res:pro[0].mat>con[0].mat?`${pro[0].engine} (${fmt(pro[0].mat)}) outweighs ${con[0].engine} (${fmt(con[0].mat)})`:`${con[0].engine} concern more material — requires mitigation`});
  if(opts.exNow&&ptR<.85)contras.push({a:"Options",b:"Prospect Theory",res:"Present option value decomposition to reframe — show strategic/learning value beyond raw NPV"});
  if(delta>0&&!optStop.frontier.every(f=>f.shouldCont))contras.push({a:"HGB (PY1+)",b:"Opt Stopping",res:"Participate with built-in review triggers at exit thresholds"});
  const fav=pro.length,ag=con.length;
  const pathway=fav>ag+2?"STRONG PARTICIPATE":fav>ag?"PARTICIPATE WITH CONDITIONS":fav===ag?"CONDITIONAL — INVESTIGATE":"DEFER";
  const pathCl=pathway.includes("STRONG")?C.pos:pathway.includes("PARTICIPATE")?cG:pathway.includes("CONDITIONAL")?C.warn:C.neg;
  return{signals,contras,pathway,pathCl,actions:signals.slice(0,6).map(s=>s.action),fav,ag,caut:signals.filter(s=>s.dir==="CAUTION"||s.dir==="NEUTRAL").length};
}

function calcInterventions(h:AheadHospital,mr:McrResult,dr:McdResult,bD:number,mV:number|null){
  const base=mr.fin+dr.fin;
  const run=(newH:AheadHospital,bd:number,mv:number|null)=>{const m=calcMcr(newH,1,bd/100);const d=calcMcd(newH,1,bd/100,mv!=null?mv/100:null);return m.fin+d.fin;};
  const ivs=[
    {nm:"CDI Program",desc:"Documentation improvement → HCC accuracy → MCR acuity",cost:800000,chain:["CDI +15pts","HCC +0.08","MCR acuity ↑","SDOH adj ↑"],apply:()=>run({...h,hcc:Math.min(2,h.hcc+.08),cdi:Math.min(100,h.cdi+15)},bD,mV)},
    {nm:"Care Coordination",desc:"3 coordinators → readmit/ED reduction → HRRP + quality",cost:450000,chain:["Readmit -2.5pp","ED -6pp","HRRP +3bp","MCR quality ↑"],apply:()=>run({...h,q:{...h.q,ra:Math.max(.05,h.q.ra-.025),ed:Math.max(.2,h.q.ed-.06),hrrp:Math.min(1,h.q.hrrp+.003)}},bD,mV)},
    {nm:"HEDIS Initiative",desc:"Quality outreach → HEDIS percentiles → MCD adjustment",cost:350000,chain:["HEDIS +6pp all","MCD adj ↑","Quality composite ↑"],apply:()=>run({...h,mcd:{...h.mcd,hedis:{pre:Math.min(.95,h.mcd.hedis.pre+.06),ed:Math.min(.95,h.mcd.hedis.ed+.06),dia:Math.min(.95,h.mcd.hedis.dia+.06),fu:Math.min(.95,h.mcd.hedis.fu+.06)}}},bD,mV)},
    {nm:"VBP Enhancement",desc:"Process improvement → VBP/HACRP scores → MCR quality",cost:275000,chain:["VBP +12bp","HACRP +5bp","MCR quality ↑"],apply:()=>run({...h,q:{...h.q,vbp:Math.min(1.04,h.q.vbp+.012),hacrp:Math.min(1,h.q.hacrp+.005)}},bD,mV)},
    {nm:"Analytics Platform",desc:"Forecasting + monitoring → demand capture → volume",cost:500000,chain:["Volume +3pp","Demand capture ↑","MCR+MCD vol ↑"],apply:()=>run(h,bD+3,mV)},
    {nm:"SDOH Staffing",desc:"Community health workers → social determinants → SRA",cost:380000,chain:["CDI +10pts","Dual capture +2pp","SRA ↑","SDOH adj ↑"],apply:()=>run({...h,cdi:Math.min(100,h.cdi+10),dp:Math.min(1,h.dp+.02)},bD,mV)},
  ];
  const singles=ivs.map(iv=>{const newFin=iv.apply();const delta=newFin-base;const roi=iv.cost>0?delta/iv.cost:0;
    return{...iv,newFin,delta,roi,payback:delta>0?iv.cost/delta:99,roiCl:roi>3?C.pos:roi>1?cG:roi>0?C.warn:C.neg};}).sort((a,b)=>b.roi-a.roi);
  const combos:{nm:string;a:string;b:string;cost:number;rawD:number;delta:number;dim:number;roi:number;roiCl:string}[]=[];const dim2=.85;
  for(let i=0;i<ivs.length;i++)for(let j=i+1;j<ivs.length;j++){
    const cost=ivs[i].cost+ivs[j].cost;const rawD=(singles.find(s=>s.nm===ivs[i].nm)?.delta||0)+(singles.find(s=>s.nm===ivs[j].nm)?.delta||0);
    const delta=rawD*dim2;const roi=cost>0?delta/cost:0;
    combos.push({nm:`${ivs[i].nm.split(" ")[0]}+${ivs[j].nm.split(" ")[0]}`,a:ivs[i].nm,b:ivs[j].nm,cost,rawD,delta,dim:dim2,roi,roiCl:roi>3?C.pos:roi>1?cG:roi>0?C.warn:C.neg});
  }
  combos.sort((a,b)=>b.roi-a.roi);
  const dim3=.75;const top3=singles.slice(0,3);const t3cost=top3.reduce((s,x)=>s+x.cost,0);const t3raw=top3.reduce((s,x)=>s+x.delta,0);
  const bestTriple={nm:top3.map(t=>t.nm.split(" ")[0]).join("+"),items:top3.map(t=>t.nm),cost:t3cost,rawD:t3raw,delta:t3raw*dim3,dim:dim3,roi:t3cost>0?t3raw*dim3/t3cost:0};
  return{singles,combos:combos.slice(0,6),bestTriple};
}

function calcStress(h:AheadHospital,bD:number,mV:number|null){
  const baseFFS=calcMcr(h,1,bD/100).ffs+calcMcd(h,1,bD/100,mV!=null?mV/100:null).ffs;
  const base=calcMcr(h,1,bD/100).fin+calcMcd(h,1,bD/100,mV!=null?mV/100:null).fin;
  const rng=seeded(h.id.split("").reduce((a,c)=>a+c.charCodeAt(0),0)+99);
  const scenarios:{id:number;sh:Record<string,number>;fin:number;delta:number;pct:number;desc:string}[]=[];
  for(let s=0;s<50;s++){
    const sh={vol:-(rng()*8),enr:-(rng()*12),vbp:-(rng()*.025),hrrp:-(rng()*.012),hedis:-(rng()*.1),hcc:-(rng()*.2),dsh:-(rng()*.3)};
    const hS={...h,hcc:Math.max(.8,h.hcc+sh.hcc),q:{...h.q,vbp:Math.max(.96,h.q.vbp+sh.vbp),hrrp:Math.max(.97,h.q.hrrp+sh.hrrp)},mcd:{...h.mcd,hedis:{pre:Math.max(.4,h.mcd.hedis.pre+sh.hedis),ed:Math.max(.4,h.mcd.hedis.ed+sh.hedis),dia:Math.max(.4,h.mcd.hedis.dia+sh.hedis),fu:Math.max(.4,h.mcd.hedis.fu+sh.hedis)},supp:{...h.mcd.supp,dsh:h.mcd.supp.dsh*(1+sh.dsh)}}};
    const mrS=calcMcr(hS,1,(bD+sh.vol)/100),drS=calcMcd(hS,1,(bD+sh.vol)/100,mV!=null?(mV+sh.enr)/100:sh.enr/100);
    const fin=mrS.fin+drS.fin,delta=fin-baseFFS;
    scenarios.push({id:s,sh,fin,delta,pct:delta/baseFFS,desc:`Vol ${sh.vol.toFixed(1)}% · Enr ${sh.enr.toFixed(1)}% · VBP ${(sh.vbp*1000).toFixed(0)}bp · HEDIS ${(sh.hedis*100).toFixed(0)}pp`});
  }
  scenarios.sort((a,b)=>a.delta-b.delta);
  const w5=scenarios.slice(0,5);const avgW5=w5.reduce((s,x)=>s+x.delta,0)/5;
  const basePos=base>baseFFS;const flipRate=scenarios.filter(s=>basePos?(s.delta<0):(s.delta>0)).length/50;
  return{worst:w5,avgW5,flipRate,basePos};
}

function calcAPM(h:AheadHospital,mr:McrResult,dr:McdResult,mp:McrResult[],dp:McdResult[],mc:McYearResult[],opts:ReturnType<typeof calcOpts>){
  const ffs=mr.ffs+dr.ffs,fin=mr.fin+dr.fin;const rev=h.bl.ip+h.bl.op+h.bl.uc;
  const qC=h.q.vbp*h.q.hrrp*h.q.hacrp;const hC=(h.mcd.hedis.pre+h.mcd.hedis.ed+h.mcd.hedis.dia+h.mcd.hedis.fu)/4;
  const aheadCum=mp.reduce((s,m,i)=>s+m.delta+dp[i].delta,0);const aheadP=mc[0]?.pA||.5;
  const aheadVar=mc[0]?.var5||0;const aheadSig=mc[0]?mc[0].spread/ffs:.1;
  const reachBench=h.tcoc.t*h.bn;const reachAct=h.tcoc.a*h.bn;const reachSav=(reachBench-reachAct)/reachBench;
  const reachShare=reachSav>0?.75:1;const reachCap=reachSav<0?Math.max(reachSav,-.05):reachSav;
  const reachDelta=reachCap*reachShare*reachBench;const reachRisk=Math.abs(reachSav)*reachBench*.3;
  const reach5=reachDelta*4.2;const reachDownP=reachSav<0?.65:reachSav<.01?.45:.25;
  const epVol=Math.round(h.bn*.12);const epCost=h.bl.ip/h.bn*1.8;
  const bpciTarget=epCost*.97;const bpciAct=epCost*(1+(h.q.ra-.15)*.5+(h.q.pqi-.05)*.3);
  const bpciSav=bpciTarget-bpciAct;const bpciDelta=bpciSav*epVol;
  const bpci5=bpciDelta*4.5;const bpciRisk=Math.abs(bpciDelta)*.4;
  const bpciDownP=bpciSav<0?.6:bpciSav/epCost<.01?.4:.2;
  const msspBench=h.tcoc.t*h.bn*.95;
  const msspEff=(qC-1)*.5+(hC-.72)*.3;const msspSav=Math.max(-.08,Math.min(.06,msspEff*.08+(reachSav*.3)));
  const msspDelta=msspSav*msspBench*.75;const msspRisk=Math.abs(msspDelta)*.25;
  const mssp5=msspDelta*4.8;const msspDownP=msspSav<0?.35:.15;
  const sp=spGet(h.st);const stBase=h.mcd.ip+h.mcd.op;
  const stQual=(hC-.7)*.04;const stDelta=stBase*Math.max(-.02,Math.min(.03,stQual));
  const st5=stDelta*4.5;const stRisk=Math.abs(stDelta)*.2;const stDownP=stQual<0?.4:.1;
  void rev; void epVol;
  const apms=[
    {nm:"AHEAD",desc:"Dual-payer global budget",type:"Global",term:"8yr",risk:"Full",delta:fin-ffs,cum5:aheadCum,downP:1-aheadP,vol:aheadSig,var5:aheadVar,sharpe:aheadSig>0?(fin-ffs)/ffs/aheadSig:0,cl:cB,rec:opts.rec},
    {nm:"ACO REACH",desc:"Total cost of care, shared savings/losses",type:"TCOC",term:"3yr",risk:"100%",delta:reachDelta,cum5:reach5,downP:reachDownP,vol:reachRisk/(rev||1),var5:-reachRisk,sharpe:reachRisk>0?reachDelta/reachRisk:0,cl:cG,rec:reachDelta>0?"Favorable":"Unfavorable"},
    {nm:"BPCI-A",desc:"Episode-based bundles, 90-day windows",type:"Episode",term:"5yr",risk:"Downside",delta:bpciDelta,cum5:bpci5,downP:bpciDownP,vol:bpciRisk/(rev||1),var5:-bpciRisk,sharpe:bpciRisk>0?bpciDelta/bpciRisk:0,cl:cM,rec:bpciDelta>0?"Favorable":"Unfavorable"},
    {nm:"MSSP",desc:"Population-based shared savings",type:"Pop",term:"5yr",risk:"Limited",delta:msspDelta,cum5:mssp5,downP:msspDownP,vol:msspRisk/(rev||1),var5:-msspRisk,sharpe:msspRisk>0?msspDelta/msspRisk:0,cl:cT,rec:msspDelta>0?"Favorable":"Conditional"},
    {nm:`${h.st} APM`,desc:`State Medicaid VBP (${sp.md})`,type:"State",term:"Annual",risk:"Low",delta:stDelta,cum5:st5,downP:stDownP,vol:stRisk/(stBase||1),var5:-stRisk,sharpe:stRisk>0?stDelta/stRisk:0,cl:cO,rec:stDelta>0?"Favorable":"Conditional"},
  ];
  const ranked=[...apms].sort((a,b)=>b.sharpe-a.sharpe);
  const dominated=apms.map(a=>apms.some(b=>b.nm!==a.nm&&b.delta>a.delta&&b.downP<a.downP));
  void dominated;
  const stackable=[{a:"AHEAD",b:"BPCI-A",note:"BPCI-A episodes excluded from AHEAD global budget — complementary"},{a:"MSSP",b:`${h.st} APM`,note:"Federal + state tracks can run simultaneously"},{a:"ACO REACH",b:`${h.st} APM`,note:"Different payer, compatible participation"}];
  const best=ranked[0];const bestCombo=apms[0].delta+apms[3].delta>apms[0].delta+apms[2].delta?{a:apms[0],b:apms[3],v:apms[0].delta+apms[3].delta}:{a:apms[0],b:apms[2],v:apms[0].delta+apms[2].delta};
  return{apms,ranked,stackable,best,bestCombo};
}

// ═══════════════════════════════════════════════════════════════════
// UI COMPONENTS + AUDIT TRAIL + DATA IMPORT
// ═══════════════════════════════════════════════════════════════════

const Card=({children,style:st,onClick}:{children:React.ReactNode;style?:React.CSSProperties;onClick?:()=>void})=><div onClick={onClick} style={{background:C.white,borderRadius:14,boxShadow:SHADOW,overflow:"hidden",transition:TR,...st}}>{children}</div>;
const CH=({title,badge,right}:{title:string;badge?:string|number|null;right?:React.ReactNode})=><div style={{padding:"14px 20px 8px",display:"flex",alignItems:"baseline",justifyContent:"space-between",flexWrap:"wrap",gap:3}}><div style={{display:"flex",alignItems:"baseline",gap:4}}><span style={{fontSize:16,fontWeight:600,color:C.ink}}>{title}</span>{badge&&<span style={{fontSize:13,fontWeight:500,color:C.inkLight,background:C.surface,padding:"1px 4px",borderRadius:100,fontFamily:FONT.mono}}>{badge}</span>}</div>{right&&<span style={{fontSize:14,color:C.inkLight,fontFamily:FONT.mono}}>{right}</span>}</div>;
const Met=({label,value,detail,trend,onClick}:{label:string;value:React.ReactNode;detail?:string;trend?:string;onClick?:()=>void})=><div style={{padding:"7px 9px",cursor:onClick?"pointer":"default"}} onClick={onClick}><div style={{fontSize:12,fontWeight:500,color:C.inkLight,textTransform:"uppercase",letterSpacing:1}}>{label}</div><div style={{fontSize:26,fontWeight:300,color:C.ink}}>{value}{onClick&&<span style={{fontSize:11,color:C.inkLight,marginLeft:2}}>▾</span>}</div>{detail&&<div style={{fontSize:13,color:trend==="up"?C.pos:trend==="down"?C.neg:C.inkLight,fontFamily:FONT.mono,fontWeight:500}}>{detail}</div>}</div>;
const NP=({active,children,onClick,small}:{active:boolean;children:React.ReactNode;onClick:()=>void;small?:boolean})=><button onClick={onClick} style={{padding:small?"5px 12px":"6px 16px",background:active?C.white:"transparent",color:active?C.ink:C.inkLight,border:"none",borderRadius:100,cursor:"pointer",fontFamily:FONT.body,fontSize:small?13:14,fontWeight:active?600:400,boxShadow:active?SHADOW:"none",transition:TR,whiteSpace:"nowrap"}}>{children}</button>;
const SubT=({active,children,onClick}:{active:boolean;children:React.ReactNode;onClick:()=>void})=><button onClick={onClick} style={{padding:"6px 12px",background:"transparent",color:active?C.ink:C.inkLight,border:"none",borderBottom:active?`2px solid ${C.ink}`:"2px solid transparent",cursor:"pointer",fontFamily:FONT.body,fontSize:22,fontWeight:active?600:400}}>{children}</button>;
const Pill=({children,color=C.inkLight}:{children:React.ReactNode;color?:string})=><span style={{display:"inline-flex",padding:"2px 8px",borderRadius:100,background:`${color}11`,color,fontSize:18,fontFamily:FONT.mono,fontWeight:500,marginLeft:2}}>{children}</span>;
const Spark=({data,color=cB,w=56,h:hh=18}:{data:number[];color?:string;w?:number;h?:number})=>{if(!data||data.length<2)return null;const mn=Math.min(...data),mx=Math.max(...data),rng=mx-mn||1;return <svg width={w} height={hh}><polyline points={data.map((v,i)=>`${i/(data.length-1)*w},${hh-(v-mn)/rng*hh}`).join(" ")} fill="none" stroke={color} strokeWidth={1.5}/></svg>;};
const Fade=({children,k}:{children:React.ReactNode;k:string})=>{const[v,setV]=useState(false);useEffect(()=>{setV(false);const t=setTimeout(()=>setV(true),20);return()=>clearTimeout(t);},[k]);return <div style={{opacity:v?1:0,transform:v?"translateY(0)":"translateY(3px)",transition:"opacity 0.2s, transform 0.2s"}}>{children}</div>;};
const Tbl=({cols,rows}:{cols:string[];rows:CellValue[][]})=><table style={{width:"100%",borderCollapse:"collapse",fontFamily:FONT.mono,fontSize:13}}><thead><tr>{cols.map((c,i)=><th key={i} style={{padding:"2px 1px",textAlign:i===0?"left":"right",color:C.inkLight,fontSize:12,borderBottom:`1px solid ${C.border}`}}>{c}</th>)}</tr></thead><tbody>{rows.map((r,i)=><tr key={i} style={{borderBottom:i<rows.length-1?`1px solid ${C.border}`:"none"}}>{r.map((c,j)=><td key={j} style={{padding:"6px 4px",textAlign:j===0?"left":"right",...(typeof c==="object"&&c!==null?c.s:{})}}>{typeof c==="object"&&c!==null?c.v:c}</td>)}</tr>)}</tbody></table>;
const WF=({data,color=cB}:{data:WaterfallStep[];color?:string})=>{const d=data.filter(x=>Math.abs(x.v)>100||x.n==="Base"||x.n==="Final").map(x=>({name:x.n,pos:x.n==="Base"||x.n==="Final"?x.v:x.v>0?x.v:0,neg:x.n==="Base"||x.n==="Final"?0:x.v<0?x.v:0,inv:x.n==="Base"||x.n==="Final"?0:x.c}));return <ResponsiveContainer width="100%" height={200}><BarChart data={d}><CartesianGrid strokeDasharray="3 3" stroke={C.border} vertical={false}/><XAxis dataKey="name" tick={{fontSize:18,fill:C.inkLight}} interval={0}/><YAxis tick={{fontSize:18,fill:C.inkLight,fontFamily:FONT.mono}} tickFormatter={v=>fmt(v as number)}/><Bar dataKey="inv" stackId="a" fill="transparent"/><Bar dataKey="pos" stackId="a" fill={`${color}CC`} radius={[2,2,0,0]}/><Bar dataKey="neg" stackId="a" fill={`${cR}99`} radius={[0,0,2,2]}/></BarChart></ResponsiveContainer>;};
const DV=({children,onClick,color}:{children:React.ReactNode;onClick?:()=>void;color?:string})=><span onClick={onClick} style={{cursor:"pointer",borderBottom:`1px dashed ${color||cT}44`,transition:TR}} onMouseEnter={e=>(e.currentTarget.style.borderBottomColor=cT)} onMouseLeave={e=>(e.currentTarget.style.borderBottomColor=`${color||cT}44`)}>{children}</span>;

function buildAudit(key:string,mr:McrResult,dr:McdResult,hosp:AheadHospital,mp?:McrResult[],dp?:McdResult[],mc?:McYearResult[]){
  const lines:{d:number;label:string;val:number|string|null;note:string}[]=[];const add=(d:number,label:string,val:number|string|null,note?:string)=>lines.push({d,label,val,note:note||""});
  if(key==="combined"||key==="all"){
    add(0,"Combined AHEAD Revenue",mr.fin+dr.fin,"MCR + MCD final");
    add(0,"Combined FFS Baseline",mr.ffs+dr.ffs,"What hospital earns without AHEAD");
    add(0,"Combined Delta",(mr.fin+dr.fin)-(mr.ffs+dr.ffs),"Revenue above/below FFS");
    add(0,"Delta %",null,fP(((mr.fin+dr.fin)-(mr.ffs+dr.ffs))/(mr.ffs+dr.ffs)));
    add(1,"MCR Component",mr.delta,"Medicare contribution to delta");
    add(1,"MCD Component",dr.delta,"Medicaid contribution to delta");
  }
  if(key==="mcr"||key==="combined"||key==="all"){
    const p=key==="mcr"?0:1;
    add(p,"MCR Final Revenue",mr.fin,"Sum of all MCR components");
    add(p+1,"Base Weighted Revenue",mr.wT,`IP ${fmt(mr.wI)} + OP ${fmt(mr.wO)} + UC ${fmt(mr.wU)}`);
    add(p+1,"× Optimization Factor",null,mr.opt.toFixed(4));
    add(p+1,"APA-Trended Volume",mr.pA,`IP ${fmt(mr.pI)} + OP ${fmt(mr.pO)} + UC ${fmt(mr.pU)}`);
    add(p+1,"+ Volume/Market Adj",mr.pV-mr.pA,`Market share × demand → ${fmt(mr.pV)}`);
    add(p+1,"+ SRA (Social Risk)",mr.sra,`CDI ${hosp.cdi} · Dual ${(hosp.dp*100).toFixed(0)}%`);
    add(p+1,"+ Quality (VBP×HRRP×HACRP)",mr.qD,`Composite: ${mr.qB.toFixed(4)}×`);
    add(p+1,"+ Efficiency",mr.eff,"PAU-based performance adjustment");
    if(mr.tA)add(p+1,"+ TCOC Adj",mr.tA,"PY4+ total cost of care");
    if(mr.hb)add(p+1,"+ Safety Net",mr.hb,"0.4% safety net hold-back");
    add(p+1,"MCR FFS Baseline",mr.ffs,"3.5% annual trend from base");
    add(p+1,"MCR Delta",mr.delta,fP(mr.pct));
  }
  if(key==="mcd"||key==="combined"||key==="all"){
    const p=key==="mcd"?0:1;
    add(p,"MCD Final Revenue",dr.fin,"Sum of all MCD components");
    add(p+1,"Base Weighted Revenue",dr.wT,`IP ${fmt(dr.wI)} + OP ${fmt(dr.wO)} + Supp ${fmt(dr.sB)}`);
    add(p+1,"Trended Volume",dr.pV,`IP trend ${fmt(dr.tI)} + OP trend ${fmt(dr.tO)}`);
    add(p+1,"+ Supplemental Trend",dr.sT-dr.sB,"DSH/Dir trended at 2.5%");
    add(p+1,"+ SDOH Adjustment",dr.sdA,`CDI ${hosp.cdi} · Dual ${(hosp.dp*100).toFixed(0)}%`);
    add(p+1,"+ HEDIS Quality",dr.hA,`Composite ${(dr.hC*100).toFixed(0)}th pctl`);
    add(p+1,"− Managed Care Offset",-dr.mcS,`${(spGet(hosp.st).mcPct*100).toFixed(0)}% MC penetration`);
    if(dr.uOn)add(p+1,"⚠ UPL Cap Applied",dr.uC,`Capped at ${spGet(hosp.st).upl}× UPL`);
    add(p+1,"MCD FFS Baseline",dr.ffs,"4% annual trend from base");
    add(p+1,"MCD Delta",dr.delta,fP(dr.pct));
  }
  if(key==="score"){
    const sc=calcSc(hosp,mr,dr,mp||[],dp||[],mc||[]);
    add(0,"Composite Score",sc.comp,`${sc.rec} — weighted factor average`);
    sc.factors.forEach(f=>add(1,`${f.nm}: ${f.sc}/10 × ${f.wt}wt`,null,f.det));
  }
  if(key==="mc"){
    const m0=mc?.[0];if(m0){
      add(0,"Monte Carlo PY1",null,"200 simulations");
      add(1,"P(> FFS)",(m0.pA*100).toFixed(1)+"%","Probability AHEAD beats FFS");
      add(1,"Median (p50)",m0.cp50,"50th percentile outcome");
      add(1,"p25 / p75",null,`${fmt(m0.cp25)} / ${fmt(m0.cp75)}`);
      add(1,"p10 / p90",null,`${fmt(m0.cp10)} / ${fmt(m0.cp90)}`);
      add(1,"VaR (5%)",m0.var5,"Worst 5th percentile");
      add(1,"CVaR (5%)",m0.cvar5,"Average of worst 5%");
      add(1,"MCR-MCD ρ",m0.rho.toFixed(3),"Cross-payer correlation");
    }
  }
  return lines;
}

const DrillPanel=({drill,onClose,mr,dr,hosp,mp,dp,mc}:{drill:string;onClose:()=>void;mr:McrResult;dr:McdResult;hosp:AheadHospital;mp:McrResult[];dp:McdResult[];mc:McYearResult[]})=>{
  const lines=buildAudit(drill,mr,dr,hosp,mp,dp,mc);
  return <Card style={{borderLeft:`3px solid ${cT}`,position:"relative"}}><CH title="Audit Trail" badge={drill.toUpperCase()} right={<span onClick={onClose} style={{cursor:"pointer",fontSize:15}}>×</span>}/>
    <div style={{padding:"0 20px 12px",maxHeight:500,overflowY:"auto"}}>
      {lines.map((l,i)=><div key={i} style={{padding:"4px 0",paddingLeft:l.d*20,borderBottom:`1px solid ${C.border}`,display:"flex",justifyContent:"space-between",alignItems:"baseline"}}>
        <div><span style={{fontSize:l.d===0?14:13,fontWeight:l.d===0?600:400,color:l.d===0?C.ink:C.inkLight}}>{l.label}</span></div>
        <div style={{textAlign:"right",minWidth:70}}>{l.val!==null&&l.val!==undefined?<span style={{fontSize:14,fontFamily:FONT.mono,fontWeight:l.d===0?600:400,color:typeof l.val==="number"?(l.val>0?C.pos:l.val<0?C.neg:C.inkLight):C.ink}}>{typeof l.val==="number"?fmt(l.val):l.val}</span>:null}
          {l.note&&<div style={{fontSize:11,color:C.inkLight,fontFamily:FONT.mono}}>{l.note}</div>}
        </div>
      </div>)}
    </div></Card>;
};

const MCHist=({mc,pyIdx=0}:{mc:McYearResult[];pyIdx?:number})=>{if(!mc||!mc[pyIdx]?.rawC)return null;
  const raw=mc[pyIdx].rawC,n=raw.length;if(n<10)return null;
  const mn=Math.min(...raw),mx=Math.max(...raw),rng=mx-mn||1,bins=20;
  const bw=rng/bins,hist=Array(bins).fill(0) as number[];
  raw.forEach(v=>{const b=Math.min(bins-1,Math.floor((v-mn)/bw));hist[b]++;});
  const maxH=Math.max(...hist),p5=raw[Math.floor(n*.05)],p25=raw[Math.floor(n*.25)],p50=raw[Math.floor(n*.5)],p75=raw[Math.floor(n*.75)],p95=raw[Math.floor(n*.95)];
  const w=480,h=120,bW=w/bins-1;
  const xOf=(v:number)=>(v-mn)/rng*w;
  const pLine=(val:number,label:string,cl:string)=>{const x=xOf(val);return <g key={label}><line x1={x} y1={0} x2={x} y2={h} stroke={cl} strokeWidth={1.2} strokeDasharray="2 1"/><text x={x} y={-2} fill={cl} fontSize={10} textAnchor="middle" fontFamily="JetBrains Mono,monospace">{label} {fmt(val)}</text></g>;};
  return <svg width={w} height={h+22} style={{overflow:"visible"}}>
    <g transform="translate(0,12)">{hist.map((c,i)=>{const x=i*(bw/rng*w),bH=maxH>0?c/maxH*h:0;const bMid=mn+i*bw+bw/2;
      return <rect key={i} x={x} y={h-bH} width={Math.max(1,bW)} height={bH} fill={bMid>0?`${C.pos}55`:`${C.neg}55`} rx={1}/>;})
    }{pLine(p5,"p5",C.neg)}{pLine(p25,"p25",cO)}{pLine(p50,"p50",cB)}{pLine(p75,"p75",cG)}{pLine(p95,"p95",C.pos)}
    <line x1={xOf(0)} y1={0} x2={xOf(0)} y2={h} stroke={C.ink} strokeWidth={1.5}/><text x={xOf(0)} y={h+9} fill={C.ink} fontSize={10} textAnchor="middle" fontFamily="JetBrains Mono,monospace">FFS=0</text>
    </g></svg>;
};

const MCTip=({active,payload}:{active?:boolean;payload?:{payload:McYearResult}[]})=>{if(!active||!payload?.length)return null;
  const d=payload[0]?.payload;if(!d)return null;
  return <div style={{background:C.white,border:`1px solid ${C.border}`,borderRadius:6,padding:"8px 12px",boxShadow:SHADOW,fontSize:12,fontFamily:FONT.mono}}>
    <div style={{fontWeight:600,marginBottom:4}}>{d.cY} (PY{d.py})</div>
    <div style={{display:"grid",gridTemplateColumns:"auto auto",gap:"2px 10px"}}>
      <span style={{color:C.inkLight}}>p90</span><span style={{color:C.pos}}>{fmt(d.cp90)}</span>
      <span style={{color:C.inkLight}}>p75</span><span>{fmt(d.cp75)}</span>
      <span style={{color:cB,fontWeight:600}}>p50</span><span style={{fontWeight:600}}>{fmt(d.cp50)}</span>
      <span style={{color:C.inkLight}}>p25</span><span>{fmt(d.cp25)}</span>
      <span style={{color:C.inkLight}}>p10</span><span style={{color:C.neg}}>{fmt(d.cp10)}</span>
      <span style={{color:"#999",borderTop:`1px solid ${C.border}`,paddingTop:1}}>FFS</span><span style={{color:"#999",borderTop:`1px solid ${C.border}`,paddingTop:1}}>{fmt(d.cffs)}</span>
      <span style={{color:C.inkLight}}>P(&gt;FFS)</span><span style={{color:d.pA>.5?C.pos:C.neg,fontWeight:600}}>{(d.pA*100).toFixed(0)}%</span>
    </div></div>;
};

// ═══════════════════════════════════════════════════════════════════
// DATA IMPORT
// ═══════════════════════════════════════════════════════════════════

const FIELD_MAP:FieldDef[]=[
  {k:"name",l:"Hospital Name",g:"core",t:"string",def:"Imported Hospital",ex:"Johns Hopkins"},
  {k:"id",l:"CMS ID / Provider #",g:"core",t:"string",def:()=>"IMP"+Date.now().toString(36).slice(-5).toUpperCase(),ex:"210009"},
  {k:"state",l:"State (AHEAD)",g:"core",t:"enum",opts:Object.keys(SP),def:"MD",ex:"MD"},
  {k:"cohort",l:"Cohort",g:"core",t:"enum",opts:[1,2,3],def:1,ex:1},
  {k:"beds",l:"Licensed Beds",g:"core",t:"num",def:300,ex:300},
  {k:"teaching",l:"Teaching Status",g:"core",t:"enum",opts:["NONE","MINOR","MAJOR"],def:"NONE",ex:"MAJOR"},
  {k:"safetyNet",l:"Safety Net",g:"core",t:"bool",def:false,ex:false},
  {k:"wageIndex",l:"Wage Index",g:"core",t:"num",def:1.0,ex:1.0396},
  {k:"benes",l:"Medicare Beneficiaries",g:"mcr",t:"num",def:8000,ex:38000},
  {k:"dualPct",l:"Dual Eligible %",g:"mcr",t:"pct",def:.15,ex:.22},
  {k:"cdi",l:"CDI Score (0-100)",g:"mcr",t:"num",def:45,ex:54},
  {k:"hcc",l:"HCC Risk Score",g:"mcr",t:"num",def:1.1,ex:1.42},
  {k:"ipRev",l:"MCR IP Revenue",g:"mcr",t:"dollar",def:120e6,ex:738e6},
  {k:"opRev",l:"MCR OP Revenue",g:"mcr",t:"dollar",def:80e6,ex:458e6},
  {k:"ucRev",l:"Uncompensated Care",g:"mcr",t:"dollar",def:4e6,ex:19e6},
  {k:"vbp",l:"VBP Adjustment",g:"quality",t:"num",def:1.0,ex:1.018},
  {k:"hrrp",l:"HRRP Penalty Factor",g:"quality",t:"num",def:.99,ex:.9921},
  {k:"hacrp",l:"HACRP Factor",g:"quality",t:"num",def:1.0,ex:1.0},
  {k:"readmit",l:"Readmission Rate",g:"quality",t:"pct",def:.15,ex:.123},
  {k:"pqi",l:"PQI Rate",g:"quality",t:"pct",def:.05,ex:.032},
  {k:"ed",l:"ED Utilization",g:"quality",t:"pct",def:.50,ex:.41},
  {k:"tcocT",l:"TCOC Target PMPM",g:"quality",t:"num",def:1100,ex:1380},
  {k:"tcocA",l:"TCOC Actual PMPM",g:"quality",t:"num",def:1100,ex:1345},
  {k:"mcdIp",l:"MCD IP Revenue",g:"mcd",t:"dollar",def:40e6,ex:280e6},
  {k:"mcdOp",l:"MCD OP Revenue",g:"mcd",t:"dollar",def:25e6,ex:180e6},
  {k:"mcdDsh",l:"DSH Payments",g:"mcd",t:"dollar",def:5e6,ex:45e6},
  {k:"mcdDir",l:"Direct GME",g:"mcd",t:"dollar",def:0,ex:12e6},
  {k:"mcdBn",l:"Medicaid Beneficiaries",g:"mcd",t:"num",def:6000,ex:42000},
  {k:"hPre",l:"HEDIS Preventive",g:"mcd",t:"pct",def:.75,ex:.82},
  {k:"hEd",l:"HEDIS ED Follow-up",g:"mcd",t:"pct",def:.72,ex:.72},
  {k:"hDia",l:"HEDIS Diabetes",g:"mcd",t:"pct",def:.75,ex:.78},
  {k:"hFu",l:"HEDIS Follow-up",g:"mcd",t:"pct",def:.68,ex:.68},
  {k:"cahCost",l:"CAH Allowable Cost",g:"mcd",t:"dollar",def:0,ex:0},
];

function parseImport(raw:string){
  const errs:string[]=[];const warnings:string[]=[];const hosps:AheadHospital[]=[];
  let data:Record<string,unknown>[]|null=null;
  try{
    const trimmed=raw.trim();
    if(trimmed.startsWith("[")||trimmed.startsWith("{")){
      const parsed=JSON.parse(trimmed) as unknown;
      if(Array.isArray(parsed))data=parsed as Record<string,unknown>[];
      else if(parsed&&typeof parsed==="object")data=[parsed as Record<string,unknown>];
    }
  }catch{data=null;}
  if(!data){
    const lines=raw.trim().split("\n").map(l=>l.split(/[,\t]/).map(c=>c.trim().replace(/^"|"$/g,"")));
    if(lines.length<2){errs.push("Need header row + at least 1 data row");return{hosps,errs,warnings};}
    const hdr=lines[0].map(h=>h.toLowerCase().replace(/[^a-z0-9]/g,""));
    data=lines.slice(1).filter(l=>l.some(c=>c)).map(row=>{const obj:Record<string,string>={};hdr.forEach((h,i)=>{if(i<row.length)obj[h]=row[i];});return obj;});
  }
  if(!data||data.length===0){errs.push("Could not parse input as JSON or CSV");return{hosps,errs,warnings};}
  data.forEach((row,ri)=>{
    const c:Record<string,number|string|boolean>={};
    FIELD_MAP.forEach(f=>{
      const keys=[f.k,f.k.toLowerCase(),f.l,f.l.toLowerCase(),f.l.replace(/\s/g,"").toLowerCase(),f.l.replace(/[^a-z0-9]/gi,"").toLowerCase()];
      let v:unknown=undefined;
      for(const k of keys){if(row[k]!==undefined&&row[k]!==""){v=row[k];break;}}
      if(v===undefined){Object.keys(row).forEach(rk=>{if(rk.toLowerCase().replace(/[^a-z0-9]/g,"")===f.k.toLowerCase())v=row[rk];});}
      if(v===undefined||v===""||v===null){
        c[f.k]=typeof f.def==="function"?f.def():f.def;
        if(["ipRev","opRev","mcdIp","mcdOp","benes","mcdBn"].includes(f.k)){warnings.push(`Row ${ri+1}: ${f.l} missing → using default`);}
      }else{
        if(f.t==="num"||f.t==="dollar"||f.t==="pct"){
          const n=parseFloat(String(v).replace(/[$,]/g,""));
          if(isNaN(n)){c[f.k]=typeof f.def==="function"?f.def():f.def;warnings.push(`Row ${ri+1}: ${f.l} "${v}" not numeric → default`);}
          else c[f.k]=n;
        }else if(f.t==="bool"){
          c[f.k]=v===true||v==="true"||v==="1"||v==="yes"||v==="Y";
        }else if(f.t==="enum"){
          const sv=String(v).toUpperCase();
          if(f.opts?.map(o=>String(o).toUpperCase()).includes(sv))c[f.k]=f.opts.find(o=>String(o).toUpperCase()===sv)!;
          else{c[f.k]=typeof f.def==="function"?f.def():f.def;warnings.push(`Row ${ri+1}: ${f.l} "${v}" not in [${f.opts}] → default`);}
        }else c[f.k]=String(v);
      }
    });
    hosps.push(cToH(c as unknown as CustomFormData));
  });
  return{hosps,errs,warnings};
}

function genTemplate(){
  const hdr=FIELD_MAP.map(f=>f.k).join(",");
  const ex=FIELD_MAP.map(f=>typeof f.ex==="string"?`"${f.ex}"`:f.ex).join(",");
  return hdr+"\n"+ex;
}

const defC:CustomFormData={name:"My Hospital",state:"MD",cohort:1,beds:300,teaching:"NONE",safetyNet:false,wageIndex:1,benes:8000,dualPct:.15,cdi:45,hcc:1.1,ipRev:120e6,opRev:80e6,ucRev:4e6,vbp:1,hrrp:.99,hacrp:1,readmit:.15,pqi:.05,ed:.5,tcocT:1100,tcocA:1100,cahCost:0,mcdIp:40e6,mcdOp:25e6,mcdDsh:5e6,mcdDir:0,mcdBn:6000,hPre:.75,hEd:.72,hDia:.75,hFu:.68};
function cToH(c:CustomFormData):AheadHospital{return{id:"CUSTOM",nm:c.name,st:c.state,co:c.cohort,ty:c.cahCost>0?"CAH":"ACH",beds:c.beds,tch:c.teaching,sn:c.safetyNet,cah:c.cahCost>0,wi:c.wageIndex,ms:.1,bn:c.benes,dp:c.dualPct,cdi:c.cdi,hcc:c.hcc,bl:{ip:c.ipRev,op:c.opRev,uc:c.ucRev},q:{vbp:c.vbp,hrrp:c.hrrp,hacrp:c.hacrp,ra:c.readmit,pqi:c.pqi,ed:c.ed},tcoc:{t:c.tcocT,a:c.tcocA},cost:c.cahCost,mcd:{ip:c.mcdIp,op:c.mcdOp,supp:{dsh:c.mcdDsh,dir:c.mcdDir},bn:c.mcdBn,hedis:{pre:c.hPre,ed:c.hEd,dia:c.hDia,fu:c.hFu}}};}

function DataImport({onImport}:{onImport:(hosps:AheadHospital[])=>void}){
  const[mode,setMode]=useState("json");const[raw,setRaw]=useState("");const[result,setResult]=useState<ReturnType<typeof parseImport>|null>(null);
  const doParse=()=>{const r=parseImport(raw);setResult(r);if(r.hosps.length>0&&r.errs.length===0)onImport(r.hosps);};
  const doTemplate=()=>{setRaw(genTemplate());setMode("csv");};
  const is:React.CSSProperties={background:C.surface,border:`1px solid ${C.border}`,borderRadius:3,color:C.ink,fontFamily:FONT.mono,fontSize:13,width:"100%",boxSizing:"border-box",outline:"none",resize:"vertical"};
  const fieldReport=useMemo(()=>{
    if(!result||result.errs.length>0||result.hosps.length===0)return null;
    const critical=["ipRev","opRev","mcdIp","mcdOp","benes","mcdBn","state","beds"];
    const defaultedKeys=new Set<string>();const matchedKeys=new Set<string>();
    result.warnings.forEach(w=>{const m=w.match(/: (.+?) (?:missing|".*?" not)/);if(m){const f=FIELD_MAP.find(ff=>ff.l===m[1]);if(f)defaultedKeys.add(f.k);}});
    FIELD_MAP.forEach(f=>{if(!defaultedKeys.has(f.k))matchedKeys.add(f.k);});
    const groups:Record<string,{k:string;l:string;status:string;matched:boolean}[]>={core:[],mcr:[],quality:[],mcd:[]};
    FIELD_MAP.forEach(f=>{
      const matched=matchedKeys.has(f.k);const isCrit=critical.includes(f.k);
      const status=matched?"matched":(isCrit?"critical-default":"default");
      groups[f.g]?.push({k:f.k,l:f.l,status,matched});
    });
    const nM=matchedKeys.size,nD=defaultedKeys.size,nC=critical.filter(k=>defaultedKeys.has(k)).length;
    return{groups,nM,nD,nC,total:FIELD_MAP.length};
  },[result]);
  const stClr=(s:string)=>s==="matched"?C.pos:s==="critical-default"?C.warn:"#94A3B8";
  const stIcon=(s:string)=>s==="matched"?"✓":s==="critical-default"?"⚠":"○";
  return(<div style={{display:"grid",gap:3}}>
    <div style={{display:"flex",gap:1}}>{([["json","JSON"],["csv","CSV"],["ref","Field Ref"]] as const).map(([k,l])=><SubT key={k} active={mode===k} onClick={()=>setMode(k)}>{l}</SubT>)}</div>
    {mode==="json"&&<div style={{display:"grid",gap:6}}>
      <div style={{fontSize:12,color:C.inkLight}}>Paste a JSON object or array. Keys map to field names below.</div>
      <textarea rows={8} value={raw} onChange={e=>setRaw(e.target.value)} placeholder={'[\n  { "name": "My Hospital", "state": "MD", ... }\n]'} style={{...is,padding:4,minHeight:80}}/>
    </div>}
    {mode==="csv"&&<div style={{display:"grid",gap:6}}>
      <div style={{display:"flex",justifyContent:"space-between",alignItems:"center"}}><span style={{fontSize:12,color:C.inkLight}}>Paste CSV with header row.</span><button onClick={doTemplate} style={{background:C.surface,border:`1px solid ${C.border}`,borderRadius:3,padding:"1px 4px",fontSize:11,cursor:"pointer",color:C.ink,fontFamily:FONT.mono}}>Load Template</button></div>
      <textarea rows={8} value={raw} onChange={e=>setRaw(e.target.value)} placeholder="name,state,cohort,beds,ipRev,opRev,..." style={{...is,padding:4,minHeight:80}}/>
    </div>}
    {mode==="ref"&&<div style={{maxHeight:200,overflowY:"auto"}}>
      {(["core","mcr","quality","mcd"] as const).map(g=><div key={g} style={{marginBottom:3}}>
        <div style={{fontSize:12,fontWeight:600,textTransform:"uppercase",color:C.ink,padding:"1px 0",borderBottom:`1px solid ${C.border}`}}>{g==="mcr"?"Medicare":g==="mcd"?"Medicaid":g==="core"?"Identity":"Quality"}</div>
        {FIELD_MAP.filter(f=>f.g===g).map(f=><div key={f.k} style={{display:"grid",gridTemplateColumns:"80px 1fr 40px 55px",fontSize:12,padding:"1px 0",borderBottom:`1px solid ${C.border}22`}}>
          <span style={{fontFamily:FONT.mono,fontWeight:500}}>{f.k}</span><span style={{color:C.inkLight}}>{f.l}</span><span style={{fontFamily:FONT.mono,color:C.inkLight,textAlign:"right"}}>{f.t}</span><span style={{fontFamily:FONT.mono,textAlign:"right",fontSize:11}}>{typeof f.ex==="number"&&(f.ex as number)>9999?fmt(f.ex as number):String(f.ex)}</span>
        </div>)}
      </div>)}
    </div>}
    {mode!=="ref"&&<div style={{display:"flex",gap:6}}>
      <button onClick={doParse} disabled={!raw.trim()} style={{flex:1,padding:"4px",background:raw.trim()?C.ink:C.border,color:C.white,border:"none",borderRadius:3,fontSize:14,fontWeight:600,cursor:raw.trim()?"pointer":"default"}}>Import & Calculate</button>
      <button onClick={()=>{setRaw("");setResult(null);}} style={{padding:"4px 8px",background:C.surface,border:`1px solid ${C.border}`,borderRadius:3,fontSize:13,cursor:"pointer",color:C.inkLight}}>Clear</button>
    </div>}
    {result&&<div style={{padding:4,background:result.errs.length>0?`${C.neg}08`:result.warnings.length>0?`${C.warn}08`:`${C.pos}08`,borderRadius:4}}>
      {result.errs.length>0&&<div style={{fontSize:13,color:C.neg,fontWeight:600}}>✗ {result.errs.join("; ")}</div>}
      {result.errs.length===0&&<div style={{fontSize:13,color:C.pos,fontWeight:600}}>✓ {result.hosps.length} hospital{result.hosps.length>1?"s":""} imported</div>}
      {result.errs.length===0&&result.hosps.length>0&&<div style={{marginTop:2}}>
        {result.hosps.map((h,i)=><div key={i} style={{fontSize:12,fontFamily:FONT.mono,padding:"1px 0"}}>{h.nm} · {h.st} · {h.beds} beds · MCR {fmt(h.bl.ip+h.bl.op)} · MCD {fmt(h.mcd.ip+h.mcd.op)}</div>)}
      </div>}
    </div>}
    {fieldReport&&<Card><CH title="Field Validation" badge={`${fieldReport.nM}/${fieldReport.total} matched`}/><div style={{padding:"0 12px 12px"}}>
      <div style={{display:"flex",gap:6,marginBottom:4,fontSize:12}}>
        <span style={{color:C.pos}}>✓ {fieldReport.nM} matched</span>
        <span style={{color:C.inkLight}}>○ {fieldReport.nD-fieldReport.nC} defaulted</span>
        {fieldReport.nC>0&&<span style={{color:C.warn}}>⚠ {fieldReport.nC} critical defaults</span>}
      </div>
      <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:12}}>
        {(["core","mcr","quality","mcd"] as const).map(g=><div key={g}>
          <div style={{fontSize:11,fontWeight:600,textTransform:"uppercase",color:C.ink,marginBottom:1}}>{g==="mcr"?"Medicare":g==="mcd"?"Medicaid":g==="core"?"Identity":"Quality"}</div>
          {fieldReport.groups[g]?.map(f=><div key={f.k} style={{display:"flex",justifyContent:"space-between",alignItems:"center",padding:"0.5px 0",fontSize:11}}>
            <span style={{color:f.matched?C.ink:C.inkLight}}>{f.l}</span>
            <span style={{color:stClr(f.status),fontFamily:FONT.mono,fontWeight:f.status==="critical-default"?600:400}}>{stIcon(f.status)}</span>
          </div>)}
        </div>)}
      </div>
    </div></Card>}
  </div>);
}

function CustForm({c,sc,onUse}:{c:CustomFormData;sc:React.Dispatch<React.SetStateAction<CustomFormData>>;onUse:()=>void}){
  const[sec,setSec]=useState("core");
  const u=(k:keyof CustomFormData,v:CustomFormData[keyof CustomFormData])=>sc(p=>({...p,[k]:v}));
  const is:React.CSSProperties={background:C.surface,border:`1px solid ${C.border}`,borderRadius:3,color:C.ink,padding:"3px 4px",fontFamily:FONT.mono,fontSize:13,width:"100%",boxSizing:"border-box",outline:"none"};
  const cb:React.CSSProperties={...is,width:"auto",marginRight:3};
  const L=({children}:{children:React.ReactNode})=><label style={{fontSize:11,fontWeight:500,color:C.inkLight,textTransform:"uppercase",letterSpacing:.5}}>{children}</label>;
  const inp=(k:keyof CustomFormData)=>({value:c[k] as string|number,onChange:(e:React.ChangeEvent<HTMLInputElement|HTMLSelectElement>)=>u(k,e.target.type==="number"?parseFloat(e.target.value)||0:e.target.value),style:is});
  const G=({cols="1fr 1fr 1fr",children}:{cols?:string;children:React.ReactNode})=><div style={{display:"grid",gridTemplateColumns:cols,gap:6}}>{children}</div>;
  return(<div style={{display:"grid",gap:2,maxHeight:"60vh",overflowY:"auto"}}>
    <div style={{display:"flex",gap:0,borderBottom:`1px solid ${C.border}`,flexWrap:"wrap"}}>{([["core","Core"],["mcr","Medicare"],["mcd","Medicaid"],["quality","Quality"],["tcoc","TCOC"]] as const).map(([k,l])=><SubT key={k} active={sec===k} onClick={()=>setSec(k)}>{l}</SubT>)}</div>
    {sec==="core"&&<><G cols="2fr 1fr 1fr"><div><L>Hospital</L><input {...inp("name")}/></div><div><L>State</L><select {...inp("state")}>{Object.keys(SP).map(s=><option key={s}>{s}</option>)}</select></div><div><L>Cohort</L><select value={c.cohort} onChange={e=>u("cohort",+e.target.value)} style={is}><option value={1}>1</option><option value={2}>2</option><option value={3}>3</option></select></div></G>
      <G cols="1fr 1fr 1fr 1fr"><div><L>Beds</L><input type="number" {...inp("beds")}/></div><div><L>Teaching</L><select {...inp("teaching")}><option>NONE</option><option>MINOR</option><option>MAJOR</option></select></div><div><L>Wage Idx</L><input type="number" step=".01" {...inp("wageIndex")}/></div><div><L>Benes</L><input type="number" {...inp("benes")}/></div></G>
      <G cols="1fr 1fr 1fr"><div><L>HCC</L><input type="number" step=".01" {...inp("hcc")}/></div><div><L>CDI Score</L><input type="number" {...inp("cdi")}/></div><div><L>Dual %</L><input type="number" step=".01" {...inp("dualPct")}/></div></G>
      <G cols="1fr 1fr"><div style={{display:"flex",alignItems:"center"}}><input type="checkbox" checked={c.safetyNet} onChange={e=>u("safetyNet",e.target.checked)} style={cb}/><L>Safety Net</L></div><div><L>CAH Cost</L><input type="number" {...inp("cahCost")}/></div></G></>}
    {sec==="mcr"&&<><G><div><L>MCR IP Rev</L><input type="number" {...inp("ipRev")}/></div><div><L>MCR OP Rev</L><input type="number" {...inp("opRev")}/></div><div><L>UC Rev</L><input type="number" {...inp("ucRev")}/></div></G></>}
    {sec==="mcd"&&<><G><div><L>MCD IP</L><input type="number" {...inp("mcdIp")}/></div><div><L>MCD OP</L><input type="number" {...inp("mcdOp")}/></div><div><L>MCD Benes</L><input type="number" {...inp("mcdBn")}/></div></G>
      <G cols="1fr 1fr"><div><L>DSH</L><input type="number" {...inp("mcdDsh")}/></div><div><L>Direct GME</L><input type="number" {...inp("mcdDir")}/></div></G>
      <G cols="1fr 1fr 1fr 1fr"><div><L>HEDIS Pre</L><input type="number" step=".01" {...inp("hPre")}/></div><div><L>HEDIS ED</L><input type="number" step=".01" {...inp("hEd")}/></div><div><L>HEDIS Dia</L><input type="number" step=".01" {...inp("hDia")}/></div><div><L>HEDIS FU</L><input type="number" step=".01" {...inp("hFu")}/></div></G></>}
    {sec==="quality"&&<><G><div><L>VBP</L><input type="number" step=".001" {...inp("vbp")}/></div><div><L>HRRP</L><input type="number" step=".001" {...inp("hrrp")}/></div><div><L>HACRP</L><input type="number" step=".001" {...inp("hacrp")}/></div></G>
      <G><div><L>Readmit %</L><input type="number" step=".01" {...inp("readmit")}/></div><div><L>PQI Rate</L><input type="number" step=".01" {...inp("pqi")}/></div><div><L>ED Util</L><input type="number" step=".01" {...inp("ed")}/></div></G></>}
    {sec==="tcoc"&&<><G cols="1fr 1fr"><div><L>TCOC Target</L><input type="number" {...inp("tcocT")}/></div><div><L>TCOC Actual</L><input type="number" {...inp("tcocA")}/></div></G>
      <div style={{fontSize:12,color:C.inkLight,padding:2}}>TCOC feeds PY4+ cost containment adjustment and ACO REACH comparison model.</div></>}
    <button onClick={onUse} style={{padding:"4px",background:C.ink,color:C.white,border:"none",borderRadius:3,fontSize:14,fontWeight:600,cursor:"pointer",marginTop:2}}>Calculate →</button></div>);}

// ═══════════════════════════════════════════════════════════════════
// MAIN COMPONENT
// ═══════════════════════════════════════════════════════════════════

export default function AheadCalculator(){
  const[view,setView]=useState("brief");const[selId,setSelId]=useState(H[0].id);const[tab,setTab]=useState("overview");const[aTab,setATab]=useState("sens");const[drill,setDrill]=useState<string|null>(null);const[showDemo,setShowDemo]=useState(true);
  const[yrs,setYrs]=useState(5);const[bD,setBD]=useState(0);const[mV,setMV]=useState<number|null>(null);
  const[vO,setVO]=useState<number|null>(null);const[hO,setHO]=useState<number|null>(null);const[aO,setAO]=useState<number|null>(null);
  const[cust,setCust]=useState<CustomFormData>(defC);const[useCust,setUseCust]=useState(false);
  const[xH,setXH]=useState<AheadHospital[]>([]);
  const builtInIds=useMemo(()=>new Set(H.map(x=>x.id)),[]);
  const allH=useMemo(()=>{
    const extra=xH.filter(x=>!builtInIds.has(x.id));
    return[...H,...extra];
  },[xH,builtInIds]);
  const[scenarios,setScenarios]=useState<Scenario[]>([]);const[scName,setScName]=useState("");const[compSc,setCompSc]=useState(false);
  const[wts,setWts]=useState<Record<string,number>>({...DEF_WT});const[showWts,setShowWts]=useState(false);
  const setWt=(k:string,v:number)=>setWts(p=>({...p,[k]:v}));const resetWts=()=>setWts({...DEF_WT});
  const wtsChanged=useMemo(()=>Object.keys(DEF_WT).some(k=>wts[k]!==DEF_WT[k]),[wts]);
  const saveScenario=()=>{if(!scName.trim())return;setScenarios(p=>[...p,{nm:scName.trim(),bD,mV,vO,hO,aO,yrs,hospId:selId,ts:Date.now()}]);setScName("");};
  const loadScenario=(sc:Scenario)=>{setBD(sc.bD);setMV(sc.mV);setVO(sc.vO);setHO(sc.hO);setAO(sc.aO);setYrs(sc.yrs);if(sc.hospId!=="CUSTOM"){setSelId(sc.hospId);setUseCust(false);}setDrill(null);};
  const[wW,setWW]=useState(typeof window!=="undefined"?window.innerWidth:1200);
  useEffect(()=>{const h=()=>setWW(window.innerWidth);window.addEventListener("resize",h);return()=>window.removeEventListener("resize",h);},[]);
  const compact=wW<900;const sideW=compact?"1fr":"minmax(260px,300px) 1fr";
  const[showSB,setShowSB]=useState(!compact);
  const reset=useCallback(()=>{setBD(0);setMV(null);setVO(null);setHO(null);setAO(null);setDrill(null);},[]);

  // ─── Computed values ───
  const hosp=useMemo(()=>useCust?cToH(cust):allH.find(x=>x.id===selId)||H[0],[selId,useCust,cust,allH]);
  const modH=useMemo(()=>{const x={...hosp,q:{...hosp.q}};if(vO!==null)x.q.vbp=vO;if(hO!==null)x.q.hrrp=hO;if(aO!==null)x.q.hacrp=aO;return x;},[hosp,vO,hO,aO]);
  const mr=useMemo(()=>calcMcr(modH,1,bD/100),[modH,bD]);
  const dr=useMemo(()=>calcMcd(modH,1,bD/100,mV!=null?mV/100:null),[modH,bD,mV]);
  const mp=useMemo(()=>pjMcr(modH,yrs,bD/100),[modH,yrs,bD]);
  const dp=useMemo(()=>pjMcd(modH,yrs,bD/100,mV!=null?mV/100:null),[modH,yrs,bD,mV]);
  const mc=useMemo(()=>runMC(modH,yrs,bD/100),[modH,yrs,bD]);
  const dec=useMemo(()=>calcSc(modH,mr,dr,mp,dp,mc,wts),[modH,mr,dr,mp,dp,mc,wts]);
  const sens=useMemo(()=>runSens(modH,bD,mV),[modH,bD,mV]);
  const be=useMemo(()=>solveBE(modH,bD,mV),[modH,bD,mV]);
  const yoy=useMemo(()=>decompYoY(mp,dp),[mp,dp]);
  const portfolio=useMemo(()=>calcPortfolio(allH),[allH]);
  const nash=useMemo(()=>calcNash(allH),[allH]);
  const shapley=useMemo(()=>calcShapley(allH),[allH]);
  const opts=useMemo(()=>calcOpts(modH,mc),[modH,mc]);
  const contract=useMemo(()=>calcContract(modH,bD,mV),[modH,bD,mV]);
  const cred=useMemo(()=>calcCred(modH,mp,dp,mc),[modH,mp,dp,mc]);
  const evpi=useMemo(()=>calcEVPI(modH,mc,mr,dr),[modH,mc,mr,dr]);
  const regimes=useMemo(()=>calcRegimes(modH,bD,mV),[modH,bD,mV]);
  const bayesian=useMemo(()=>calcBayesian(modH,mc,mp,dp),[modH,mc,mp,dp]);
  const markov=useMemo(()=>calcMarkov(modH),[modH]);
  const prospect=useMemo(()=>calcProspect(mc),[mc]);
  const copula=useMemo(()=>calcCopula(mc),[mc]);
  const optStop=useMemo(()=>calcOptStop(modH,mc,mp,dp),[modH,mc,mp,dp]);
  const svc=useMemo(()=>svcLine(modH),[modH]);
  const timing=useMemo(()=>cohortTime(modH),[modH]);
  const synth=useMemo(()=>calcSynthesis(modH,mr,dr,mp,dp,mc,dec,opts,prospect,bayesian,cred,nash,evpi,regimes,contract,optStop,copula,markov),[modH,mr,dr,mp,dp,mc,dec,opts,prospect,bayesian,cred,nash,evpi,regimes,contract,optStop,copula,markov]);
  const interventions=useMemo(()=>calcInterventions(modH,mr,dr,bD,mV),[modH,mr,dr,bD,mV]);
  const stress=useMemo(()=>calcStress(modH,bD,mV),[modH,bD,mV]);
  const apm=useMemo(()=>calcAPM(modH,mr,dr,mp,dp,mc,opts),[modH,mr,dr,mp,dp,mc,opts]);
  const sp=spGet(hosp.st);const cF=mr.fin+dr.fin,cFF=mr.ffs+dr.ffs,cD=cF-cFF,cPct=cD/cFF;
  const peerBench=useMemo(()=>{const all=allH.map(x=>{const r=calcMcr(x,1),d=calcMcd(x,1);return{id:x.id,nm:x.nm,delta:r.delta+d.delta,pct:(r.fin+d.fin-r.ffs-d.ffs)/(r.ffs+d.ffs)};}).sort((a,b)=>b.delta-a.delta);const rank=all.findIndex(x=>x.id===modH.id)+1;const vals=all.map(x=>x.delta);const mu=vals.reduce((s,v)=>s+v,0)/vals.length;const sig=Math.sqrt(vals.reduce((s,v)=>s+(v-mu)**2,0)/vals.length);const z=sig>0?(cD-mu)/sig:0;return{rank,of:all.length,pctile:Math.round((1-rank/all.length)*100),z,mu,sig};},[modH,cD,allH]);
  const sensTS=useMemo(()=>runSensTS(modH,yrs,bD,mV),[modH,yrs,bD,mV]);
  const sparks=useMemo(()=>{const m:Record<string,number[]>={};allH.forEach(x=>{const p=pjMcr(x,5),d=pjMcd(x,5);m[x.id]=p.map((r,i)=>r.fin+d[i].fin);});return m;},[allH]);
  void sp;

  // ─── Sidebar ───
  const Sidebar=()=><>{compact&&<button onClick={()=>setShowSB(!showSB)} style={{background:C.surface,border:`1px solid ${C.border}`,borderRadius:6,padding:"6px 12px",fontSize:12,color:C.ink,cursor:"pointer",marginBottom:4,fontFamily:FONT.mono}}>{showSB?"▲ Hide sidebar":"▼ "+hosp.nm+" · "+hosp.st}</button>}
    {(!compact||showSB)&&<div style={{display:"grid",gap:8,alignContent:"start"}}>
    <Card><CH title="Hospitals" badge={allH.length}/><div style={{padding:"0 8px 6px",maxHeight:400,overflowY:"auto"}}>{allH.map(x=>{const sk=sparks[x.id]||[];const r0=calcMcr(x,1),d0=calcMcd(x,1);const c0=r0.fin+d0.fin,cf0=r0.ffs+d0.ffs;void sk;
      return <div key={x.id} onClick={()=>{setSelId(x.id);setUseCust(false);reset();}} style={{padding:"6px 10px",cursor:"pointer",background:selId===x.id&&!useCust?C.surface:"transparent"}}>
        <div style={{display:"flex",justifyContent:"space-between",alignItems:"center"}}><span style={{fontSize:13,fontWeight:selId===x.id?600:400}}>{x.nm.length>14?x.nm.slice(0,14)+"…":x.nm}</span><Spark data={sparks[x.id]||[]} color={(c0-cf0)>0?C.pos:C.neg}/></div></div>})}</div></Card>
    <Card><CH title="Controls"/><div style={{padding:"0 8px 8px",display:"grid",gap:12}}>
      {([[bD,setBD,"Vol",-10,10],[mV||0,(v:number)=>setMV(v||null),"Enr",-15,15]] as [number,(v:number)=>void,string,number,number][]).map(([v,fn,l,mn,mx],i)=>
        <div key={i}><div style={{display:"flex",justifyContent:"space-between",fontSize:18,color:C.inkLight}}><span>{l}</span><span style={{fontFamily:FONT.mono}}>{v}%</span></div><input type="range" min={mn} max={mx} value={v} onChange={e=>fn(+e.target.value)} style={{width:"100%",accentColor:C.ink,height:4}}/></div>)}
      <div><div style={{display:"flex",justifyContent:"space-between",fontSize:18,color:C.inkLight}}><span>VBP</span><span style={{fontFamily:FONT.mono}}>{(vO??hosp.q.vbp).toFixed(3)}</span></div><input type="range" min={960} max={1040} value={Math.round((vO??hosp.q.vbp)*1000)} onChange={e=>setVO(+e.target.value/1000)} style={{width:"100%",accentColor:C.ink,height:4}}/></div>
      <div><div style={{display:"flex",justifyContent:"space-between",fontSize:18,color:C.inkLight}}><span>Yrs</span><span style={{fontFamily:FONT.mono}}>{yrs}</span></div><input type="range" min={1} max={10} value={yrs} onChange={e=>setYrs(+e.target.value)} style={{width:"100%",accentColor:C.ink,height:4}}/></div>
    </div></Card>
    <Card><CH title="Scenarios" badge={scenarios.length||null}/><div style={{padding:"0 8px 8px",display:"grid",gap:12}}>
      <div style={{display:"flex",gap:1}}><input value={scName} onChange={e=>setScName(e.target.value)} placeholder="Name this scenario" style={{background:C.surface,border:`1px solid ${C.border}`,borderRadius:4,padding:"6px 8px",fontSize:12,fontFamily:FONT.mono,flex:1,outline:"none",color:C.ink}}/><button onClick={saveScenario} style={{background:C.ink,color:"#fff",border:"none",borderRadius:4,padding:"6px 12px",fontSize:12,cursor:"pointer",fontWeight:600}}>Save</button></div>
      {scenarios.length>0&&<div style={{maxHeight:200,overflowY:"auto"}}>{scenarios.map((sc,i)=><div key={i} onClick={()=>loadScenario(sc)} style={{padding:"6px 8px",cursor:"pointer",fontSize:12,display:"flex",justifyContent:"space-between",alignItems:"center",borderBottom:`1px solid ${C.border}`}} onMouseEnter={e=>e.currentTarget.style.background=C.surface} onMouseLeave={e=>e.currentTarget.style.background="transparent"}>
        <span style={{fontWeight:500}}>{sc.nm}</span>
        <span style={{fontSize:11,color:C.inkLight,fontFamily:FONT.mono}}>Vol{sc.bD>0?"+":""}{sc.bD}% · {sc.yrs}Y</span>
      </div>)}</div>}
      {scenarios.length>=2&&<button onClick={()=>setCompSc(!compSc)} style={{background:compSc?C.ink:C.surface,color:compSc?"#fff":C.ink,border:`1px solid ${compSc?C.ink:C.border}`,borderRadius:3,fontSize:12,padding:"6px",cursor:"pointer",fontWeight:500}}>
        {compSc?"Hide":"Compare"} Scenarios
      </button>}
    </div></Card></div>}</>;

  const scComp=useMemo(()=>{if(!compSc||scenarios.length<2)return null;
    return scenarios.map(sc=>{const h=allH.find(x=>x.id===sc.hospId)||modH;const hx={...h,q:{...h.q}};if(sc.vO!==null)hx.q.vbp=sc.vO;if(sc.hO!==null)hx.q.hrrp=sc.hO;if(sc.aO!==null)hx.q.hacrp=sc.aO;
      const r=calcMcr(hx,1,sc.bD/100),d=calcMcd(hx,1,sc.bD/100,sc.mV!=null?sc.mV/100:null);
      const c=r.fin+d.fin,cf=r.ffs+d.ffs;return{nm:sc.nm,fin:c,ffs:cf,delta:c-cf,pct:(c-cf)/cf,mcr:r.fin,mcd:d.fin};});
  },[compSc,scenarios,allH,modH]);

  return(<div style={{padding:"12px 24px",maxWidth:1400,margin:"0 auto"}}>
    <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",marginBottom:8,flexWrap:"wrap",gap:6}}>
      <div style={{display:"flex",alignItems:"center",gap:4}}><button onClick={()=>{window.location.hash="/";}} style={{background:"none",border:"none",color:C.inkLight,cursor:"pointer",fontSize:14,padding:0}}>← Back</button><Pill color={cG}>v11</Pill></div>
      <div style={{display:"flex",gap:2,background:C.surface,borderRadius:100,padding:3,flexWrap:"wrap"}}>
        {([["brief","Brief"],["dashboard","Dashboard"],["intervene","Intervene"],["apm","APM Compare"],["compare","Portfolio"],["custom","Import"]] as const).map(([k,l])=><NP key={k} active={view===k} onClick={()=>{setView(k);if(k!=="custom")setUseCust(false);}} small>{l}</NP>)}
      </div>
    </div>
    {showDemo&&<div style={{display:"flex",alignItems:"center",justifyContent:"space-between",gap:12,padding:"6px 14px",marginBottom:8,borderRadius:8,background:`${C.warn}11`,border:`1px solid ${C.warn}33`}}>
      <div style={{fontSize:12,color:C.warn,lineHeight:1.5}}>
        <span style={{fontWeight:700,fontFamily:FONT.mono,letterSpacing:.5}}>DEMO MODE</span>
        <span style={{margin:"0 6px",opacity:.5}}>—</span>
        Showing 12 sample hospitals with illustrative data calibrated to public CMS sources. Import your own data for production analysis.
        <button onClick={()=>{setView("custom");}} style={{background:"none",border:"none",color:C.accent,cursor:"pointer",fontSize:12,fontWeight:600,padding:"0 0 0 6px",fontFamily:FONT.body}}>Import Data →</button>
      </div>
      <button onClick={()=>setShowDemo(false)} style={{background:"none",border:"none",color:C.warn,cursor:"pointer",fontSize:14,padding:0,flexShrink:0,opacity:.6}}>✕</button>
    </div>}
    <Fade k={view}>

    {/* ═══════════════ EXECUTIVE BRIEF ═══════════════ */}
    {view==="brief"&&<div style={{display:"grid",gridTemplateColumns:sideW,gap:12}}>
      <Sidebar/>
      <div style={{display:"grid",gap:12}}>
        <Card style={{borderLeft:`4px solid ${synth.pathCl}`}}><div style={{padding:"16px 20px",display:"flex",justifyContent:"space-between",alignItems:"center",flexWrap:"wrap",gap:12}}>
          <div><div style={{fontSize:12,color:C.inkLight,fontFamily:FONT.mono,textTransform:"uppercase",letterSpacing:1}}>AHEAD STRATEGIC ADVISORY</div>
            <div style={{fontSize:28,fontWeight:300,color:C.ink}}>{hosp.nm}{builtInIds.has(hosp.id)&&!useCust&&<Pill color={C.warn}>DEMO</Pill>}</div>
            <div style={{fontSize:13,color:C.inkLight}}>{hosp.st} · Cohort {hosp.co} · {hosp.beds} beds · {mr.cY}</div></div>
          <div style={{textAlign:"right"}}><div style={{fontSize:36,fontWeight:200,color:synth.pathCl}}>{synth.pathway}</div>
            <div style={{fontSize:13,color:C.inkLight,fontFamily:FONT.mono}}>Score {dec.comp} · {synth.fav} favor · {synth.ag} against · {synth.caut} caution</div></div>
        </div></Card>

        {compSc&&scComp&&scComp.length>=2&&<Card style={{borderLeft:`3px solid ${cT}`}}><CH title="Scenario Comparison" badge={scComp.length+" scenarios"}/><div style={{padding:"0 16px 12px"}}>
          <div style={{display:"grid",gap:6}}>
            <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:12}}>
              <div><Tbl cols={["Scenario","Combined","Δ","Δ%"]} rows={scComp.map(sc=>[{v:sc.nm,s:{fontWeight:600}},{v:fmt(sc.fin),s:{fontFamily:FONT.mono}},{v:fmt(sc.delta),s:{color:sc.delta>0?C.pos:C.neg,fontWeight:600}},{v:fP(sc.pct),s:{color:sc.pct>0?C.pos:C.neg}}])}/></div>
              <div><ResponsiveContainer width="100%" height={scComp.length*22+10}><BarChart data={scComp} layout="vertical" margin={{left:70}}>
                <CartesianGrid strokeDasharray="3 3" stroke={C.border} horizontal={false}/>
                <XAxis type="number" tick={{fontSize:11,fill:C.inkLight,fontFamily:FONT.mono}} tickFormatter={v=>fmt(v as number)}/>
                <YAxis type="category" dataKey="nm" tick={{fontSize:13,fill:C.ink}} width={65}/>
                <ReferenceLine x={0} stroke={cR} strokeDasharray="3 2"/>
                <Bar dataKey="delta" radius={[0,3,3,0]}>{scComp.map((sc,i)=><Cell key={i} fill={sc.delta>0?C.pos:C.neg}/>)}</Bar>
              </BarChart></ResponsiveContainer></div>
            </div>
            <div style={{fontSize:11,color:C.inkLight}}>Range: {fmt(Math.min(...scComp.map(s=>s.delta)))} to {fmt(Math.max(...scComp.map(s=>s.delta)))} · Spread: {fmt(Math.max(...scComp.map(s=>s.delta))-Math.min(...scComp.map(s=>s.delta)))}</div>
          </div>
        </div></Card>}

        <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit, minmax(140px, 1fr))",gap:8}}>
          {[{l:"Combined",v:fmt(cF),d:fP(cPct),cl:cPct>0?C.pos:C.neg,k:"combined"},{l:"MCR",v:fmt(mr.fin),d:fP(mr.pct),cl:mr.pct>0?C.pos:C.neg,k:"mcr"},{l:"MCD",v:fmt(dr.fin),d:fP(dr.pct),cl:dr.pct>0?C.pos:C.neg,k:"mcd"},{l:"P(>FFS)",v:((mc[0]?.pA||0)*100).toFixed(0)+"%",d:"200 sims",cl:(mc[0]?.pA||0)>.6?C.pos:C.neg,k:"mc"},{l:"Peer Rank",v:`#${peerBench.rank}/${peerBench.of}`,d:`${peerBench.z>0?"+":""}${peerBench.z.toFixed(1)}σ · p${peerBench.pctile}`,cl:peerBench.z>0?C.pos:C.neg},{l:"Best APM",v:apm.best.nm,d:`Sharpe ${apm.best.sharpe.toFixed(2)}`,cl:apm.best.delta>0?C.pos:C.neg}].map((m,i)=>
            <Card key={i} style={{boxShadow:"none",background:C.surface,cursor:m.k?"pointer":"default"}} onClick={m.k?()=>setDrill(drill===m.k?null:m.k):undefined}><div style={{padding:"10px 12px",textAlign:"center"}}><div style={{fontSize:11,color:C.inkLight,textTransform:"uppercase"}}>{m.l}</div><div style={{fontSize:16,fontWeight:300,color:m.cl}}>{m.k?<DV onClick={()=>setDrill(drill===m.k?null:m.k)}>{m.v}</DV>:m.v}</div><div style={{fontSize:11,color:C.inkLight,fontFamily:FONT.mono}}>{m.d}{m.k&&<span style={{marginLeft:2}}>▾</span>}</div></div></Card>)}
        </div>
        {drill&&<DrillPanel drill={drill} onClose={()=>setDrill(null)} mr={mr} dr={dr} hosp={modH} mp={mp} dp={dp} mc={mc}/>}

        <Card style={{borderLeft:wtsChanged?`3px solid ${cT}`:`3px solid ${C.border}`}}>
          <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",padding:"8px 16px 0"}}>
            <div style={{display:"flex",alignItems:"center",gap:12}}>
              <span style={{fontSize:13,fontWeight:600,color:C.ink}}>Synthesis Weights</span>
              {wtsChanged&&<Pill color={cT}>CUSTOM</Pill>}
            </div>
            <div style={{display:"flex",gap:6}}>
              {wtsChanged&&<button onClick={resetWts} style={{background:C.surface,border:`1px solid ${C.border}`,borderRadius:4,padding:"4px 10px",fontSize:11,cursor:"pointer",color:C.inkLight,fontFamily:FONT.mono}}>Reset</button>}
              <button onClick={()=>setShowWts(!showWts)} style={{background:"transparent",border:"none",cursor:"pointer",fontSize:12,color:C.inkLight}}>{showWts?"▲":"▼"}</button>
            </div>
          </div>
          <div style={{padding:"6px 16px 8px",display:"flex",gap:1,alignItems:"end"}}>
            {dec.factors.map((f:{nm:string;sc:number;wt:number;cl:string},i:number)=>{
              return <div key={i} style={{flex:f.wt||1,textAlign:"center"}} title={`${f.nm}: ${f.sc}/10 × ${f.wt}wt`}>
                <div style={{fontSize:11,color:C.inkLight,marginBottom:1}}>{f.nm}</div>
                <div style={{height:Math.max(6,f.sc*4),background:f.cl+"88",borderRadius:"2px 2px 0 0",transition:TR}}/>
                <div style={{fontSize:11,fontFamily:FONT.mono,color:f.cl,fontWeight:600,marginTop:1}}>{f.sc}</div>
              </div>;})}
          </div>
          <div style={{padding:"0 16px 4px",display:"flex",justifyContent:"space-between",fontSize:11,color:C.inkLight}}>
            <span>Score: <span style={{fontWeight:700,color:dec.recCl,fontSize:13}}>{dec.comp}</span> — {dec.rec}</span>
            <span style={{fontFamily:FONT.mono}}>Σwt = {Object.values(wts).reduce((a,v)=>a+v,0)}</span>
          </div>
          {showWts&&<div style={{padding:"8px 16px 14px",borderTop:`1px solid ${C.border}`}}>
            <div style={{fontSize:11,color:C.inkLight,marginBottom:3}}>Adjust weights to see how the recommendation shifts. Higher weight = more influence on composite score.</div>
            <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:"8px 20px"}}>
              {Object.keys(DEF_WT).map(k=>{const f=dec.factors.find((x:{nm:string;sc:number;cl:string})=>x.nm===k);
                return <div key={k}>
                  <div style={{display:"flex",justifyContent:"space-between",alignItems:"baseline",fontSize:12}}>
                    <span style={{fontWeight:500,color:C.ink}}>{k}</span>
                    <span style={{fontFamily:FONT.mono,fontSize:12,color:wts[k]!==DEF_WT[k]?cT:C.inkLight}}>{wts[k]} <span style={{fontSize:9,color:C.inkLight}}>({DEF_WT[k]})</span></span>
                  </div>
                  <div style={{display:"flex",alignItems:"center",gap:3}}>
                    <input type="range" min={0} max={30} value={wts[k]} onChange={e=>setWt(k,+e.target.value)} style={{flex:1,accentColor:wts[k]!==DEF_WT[k]?cT:C.ink,height:1}}/>
                    {f&&<span style={{fontSize:11,fontFamily:FONT.mono,color:f.cl,minWidth:18,textAlign:"right"}}>{f.sc}/10</span>}
                  </div>
                </div>;})}
            </div>
            <div style={{marginTop:4,padding:"3px 5px",background:C.surface,borderRadius:3,fontSize:11,color:C.inkLight}}>
              {(()=>{const defScore=calcSc(modH,mr,dr,mp,dp,mc,DEF_WT).comp;return wtsChanged?<span>Custom weights shift score from <span style={{fontWeight:600}}>{defScore}</span> (default) → <span style={{fontWeight:600,color:dec.recCl}}>{dec.comp}</span> (custom). {dec.comp>defScore?"Weighting favors participation.":"Weighting increases caution."}</span>
              :"Default empirical weights. Financial (MCR+MCD+Multi-Yr) = 50%. Risk (P>FFS+SDOH) = 30%. Quality = 20%.";})()}
            </div>
          </div>}
        </Card>

        <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:8}}>
          <Card><CH title="Projection Fan" badge={`${yrs}Y`}/><div style={{padding:"0 16px 10px"}}><ResponsiveContainer width="100%" height={200}><ComposedChart data={mc}>
            <CartesianGrid strokeDasharray="3 3" stroke={C.border} vertical={false}/>
            <XAxis dataKey="cY" tick={{fontSize:12,fill:C.inkLight,fontFamily:FONT.mono}}/><YAxis tick={{fontSize:11,fill:C.inkLight,fontFamily:FONT.mono}} tickFormatter={v=>fmt(v as number)}/>
            <Area type="monotone" dataKey="cp10" stackId="f1" fill="transparent" stroke="transparent"/>
            <Area type="monotone" dataKey="cp90" fill={`${cB}06`} stroke={`${cB}20`} strokeWidth={.5}/>
            <Area type="monotone" dataKey="cp25" stackId="f2" fill="transparent" stroke="transparent"/>
            <Area type="monotone" dataKey="cp75" fill={`${cB}12`} stroke={`${cB}30`} strokeWidth={.5}/>
            <Line type="monotone" dataKey="cp50" stroke={cB} strokeWidth={2} dot={false} name="Median"/>
            <Line type="monotone" dataKey="cffs" stroke="#9CA3AF" strokeWidth={1.5} strokeDasharray="5 3" dot={false} name="FFS"/>
            <Tooltip content={<MCTip/>}/>
            <Legend wrapperStyle={{fontSize:18}}/>
          </ComposedChart></ResponsiveContainer></div>
          <div style={{padding:"6px 16px 10px"}}><div style={{fontSize:18,color:C.inkLight,marginBottom:4,fontWeight:500}}>PY1 Delta Distribution (200 simulations)</div><MCHist mc={mc} pyIdx={0}/></div></Card>
          <Card><CH title="Cumulative Delta" badge="EROSION TRACKER"/><div style={{padding:"0 16px 10px"}}>{(()=>{let cum=0;const eros=mp.map((m,i)=>{const d=dp[i];cum+=m.delta+d.delta;return{cY:m.cY,py:m.delta+d.delta,cum};});return <ResponsiveContainer width="100%" height={200}><ComposedChart data={eros}>
            <CartesianGrid strokeDasharray="3 3" stroke={C.border} vertical={false}/>
            <XAxis dataKey="cY" tick={{fontSize:12,fill:C.inkLight,fontFamily:FONT.mono}}/><YAxis tick={{fontSize:11,fill:C.inkLight,fontFamily:FONT.mono}} tickFormatter={v=>fmt(v as number)}/>
            <ReferenceLine y={0} stroke={cR} strokeDasharray="3 2"/>
            <Bar dataKey="py" fill={`${cB}44`} name="Annual" radius={[2,2,0,0]}/>
            <Line type="monotone" dataKey="cum" stroke={cG} strokeWidth={2.5} dot={{r:2,fill:cG}} name="Cumulative"/>
            <Legend wrapperStyle={{fontSize:18}}/>
          </ComposedChart></ResponsiveContainer>})()}</div></Card>
        </div>

        <Card><CH title="Integrated Analysis" badge="12 ENGINES" right={`${synth.signals.length} signals`}/><div style={{padding:"0 16px 12px"}}>
          {synth.signals.slice(0,8).map((s:{dir:string;engine:string;finding:string;mat:number;action:string},i:number)=>{const dCl=s.dir==="FAVOR"?C.pos:s.dir==="AGAINST"?C.neg:s.dir==="DEFER"?cT:C.warn;
            return <div key={i} style={{padding:"8px 0",borderBottom:i<7?`1px solid ${C.border}`:"none"}}>
              <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",gap:3}}>
                <div style={{flex:1}}><div style={{display:"flex",alignItems:"center",gap:3}}><span style={{fontSize:10,fontWeight:600,color:dCl,background:`${dCl}11`,padding:"2px 6px",borderRadius:3,textTransform:"uppercase"}}>{s.dir}</span><span style={{fontSize:13,fontWeight:600}}>{s.engine}</span></div>
                  <div style={{fontSize:12,color:C.inkLight,marginTop:1}}>{s.finding}</div></div>
                <div style={{fontSize:12,color:C.inkLight,fontFamily:FONT.mono,minWidth:40,textAlign:"right"}}>{fmt(s.mat)}</div>
              </div>
              <div style={{fontSize:12,color:cG,fontStyle:"italic",marginTop:1}}>→ {s.action}</div>
            </div>})}
        </div></Card>

        {synth.contras.length>0&&<Card style={{borderLeft:`3px solid ${C.warn}`}}><CH title="Contradictions" badge={synth.contras.length}/><div style={{padding:"0 16px 12px"}}>
          {synth.contras.map((c:{a:string;b:string;res:string},i:number)=><div key={i} style={{padding:"3px 0"}}>
            <div style={{fontSize:13}}><span style={{color:C.pos,fontWeight:600}}>{c.a}</span> vs <span style={{color:C.neg,fontWeight:600}}>{c.b}</span></div>
            <div style={{fontSize:12,color:cG,fontStyle:"italic"}}>→ {c.res}</div>
          </div>)}</div></Card>}

        <div style={{display:"grid",gridTemplateColumns:compact?"1fr":"1fr 1fr 1fr",gap:8}}>
          <Card><CH title="APM Compare" badge={apm.apms.length}/><div style={{padding:"0 14px 10px"}}>
            {apm.ranked.slice(0,4).map((a:{nm:string;cl:string;type:string;risk:string;delta:number;sharpe:number},i:number)=><div key={i} style={{padding:"2px 0",borderBottom:i<3?`1px solid ${C.border}`:"none",display:"flex",justifyContent:"space-between",alignItems:"center"}}>
              <div><div style={{fontSize:12,fontWeight:600,color:a.cl}}>{a.nm}</div><div style={{fontSize:11,color:C.inkLight}}>{a.type} · {a.risk}</div></div>
              <div style={{textAlign:"right"}}><div style={{fontSize:14,fontWeight:300,color:a.delta>0?C.pos:C.neg}}>{fmt(a.delta)}</div><div style={{fontSize:11,fontFamily:FONT.mono,color:C.inkLight}}>S:{a.sharpe.toFixed(2)}</div></div>
            </div>)}
            <div style={{fontSize:11,color:cG,marginTop:2,fontStyle:"italic"}}>Best risk-adj: {apm.best.nm}</div>
          </div></Card>
          <Card><CH title="Stress Test" badge="50" right={`${(stress.flipRate*100).toFixed(0)}% flip`}/><div style={{padding:"0 14px 10px"}}>
            <Tbl cols={["Scenario","Δ"]} rows={stress.worst.slice(0,3).map((s:{desc:string;delta:number})=>[{v:s.desc,s:{fontSize:11,fontFamily:FONT.body}},{v:fmt(s.delta),s:{color:C.neg,fontWeight:600}}])}/>
            <div style={{fontSize:11,color:C.inkLight,marginTop:1}}>Avg worst-5: {fmt(stress.avgW5)}</div>
          </div></Card>
          <Card><CH title="Interventions" badge="ROI"/><div style={{padding:"0 14px 10px"}}>
            <Tbl cols={["Invest","ROI","Δ"]} rows={interventions.singles.slice(0,3).map((iv:{nm:string;roi:number;roiCl:string;delta:number})=>[{v:iv.nm,s:{fontWeight:500,fontSize:11}},{v:iv.roi.toFixed(1)+"×",s:{color:iv.roiCl,fontWeight:600}},{v:fmt(iv.delta),s:{color:iv.delta>0?C.pos:C.neg}}])}/>
          </div></Card>
        </div>

        <Card style={{background:`${synth.pathCl}08`,boxShadow:"none"}}><div style={{padding:"14px 20px"}}>
          <div style={{fontSize:14,fontWeight:600,color:synth.pathCl,marginBottom:4}}>Recommended Actions</div>
          {synth.actions.map((a:string,i:number)=><div key={i} style={{fontSize:13,color:C.ink,padding:"4px 0",paddingLeft:14,borderLeft:`2px solid ${synth.pathCl}44`}}>{i+1}. {a}</div>)}
        </div></Card>

        <Card><CH title={`${yrs}Y Projection`}/><div style={{padding:"0 14px 4px"}}><Tbl cols={["PY","MCR","MCD","Comb","Δ%","P(>)","VaR"]} rows={mp.map((m,i)=>{const d=dp[i],cc=m.fin+d.fin,cf=m.ffs+d.ffs;return[`PY${m.py}`,fmt(m.fin),{v:fmt(d.fin),s:{color:cM}},{v:fmt(cc),s:{fontWeight:600}},{v:fP((cc-cf)/cf),s:{color:(cc-cf)>0?C.pos:C.neg}},((mc[i]?.pA||0)*100).toFixed(0)+"%",{v:fmt(mc[i]?.var5||0),s:{color:C.neg,fontSize:11}}];})}/></div></Card>
      </div>
    </div>}

    {/* ═══════════════ INTERVENTIONS ═══════════════ */}
    {view==="intervene"&&<div style={{display:"grid",gridTemplateColumns:sideW,gap:12}}>
      <Sidebar/>
      <div style={{display:"grid",gap:12}}>
        <Card><CH title="Individual Interventions" badge="OPERATIONAL → ANALYTICAL" right={`${interventions.singles.filter((iv:{roi:number})=>iv.roi>1).length} positive-ROI`}/><div style={{padding:"0 16px 12px"}}>
          {interventions.singles.map((iv:{nm:string;desc:string;roi:number;roiCl:string;cost:number;delta:number;chain:string[];payback:number;newFin:number},i:number)=><div key={i} style={{padding:"10px 0",borderBottom:i<interventions.singles.length-1?`1px solid ${C.border}`:"none"}}>
            <div style={{display:"flex",justifyContent:"space-between",alignItems:"center"}}>
              <div><div style={{fontSize:15,fontWeight:600}}>{iv.nm}</div><div style={{fontSize:12,color:C.inkLight}}>{iv.desc}</div></div>
              <div style={{textAlign:"right"}}><div style={{fontSize:22,fontWeight:300,color:iv.roiCl}}>{iv.roi.toFixed(1)}× ROI</div><div style={{fontSize:12,fontFamily:FONT.mono,color:C.inkLight}}>{fmt(iv.cost)} → {fmt(iv.delta)}</div></div>
            </div>
            <div style={{display:"flex",gap:2,marginTop:2,flexWrap:"wrap"}}>{iv.chain.map((c:string,j:number)=><div key={j} style={{display:"flex",alignItems:"center",gap:1}}><span style={{fontSize:11,padding:"1px 3px",background:C.surface,borderRadius:2,fontFamily:FONT.mono}}>{c}</span>{j<iv.chain.length-1&&<span style={{fontSize:11,color:C.inkLight}}>→</span>}</div>)}</div>
            <div style={{display:"flex",gap:6,marginTop:2,fontSize:11,fontFamily:FONT.mono,color:C.inkLight}}>
              <span>Payback: {iv.payback<1?`${(iv.payback*12).toFixed(0)}mo`:`${iv.payback.toFixed(1)}yr`}</span><span>New combined: {fmt(iv.newFin)}</span>
            </div>
          </div>)}
        </div></Card>
        <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:12}}>
          <Card><CH title="Revenue Impact"/><div style={{padding:"0 14px 10px"}}><ResponsiveContainer width="100%" height={200}><BarChart data={interventions.singles.map((iv:{nm:string;delta:number;cost:number})=>({nm:iv.nm.split(" ")[0],delta:iv.delta,cost:-iv.cost}))}><CartesianGrid strokeDasharray="3 3" stroke={C.border} vertical={false}/><XAxis dataKey="nm" tick={{fontSize:12,fill:C.inkLight}}/><YAxis tick={{fontSize:11,fill:C.inkLight,fontFamily:FONT.mono}} tickFormatter={v=>fmt(v as number)}/>
            <ReferenceLine y={0} stroke={cR} strokeDasharray="3 2"/><Bar dataKey="delta" fill={`${C.pos}CC`} name="Δ Rev" radius={[2,2,0,0]}/><Bar dataKey="cost" fill={`${C.neg}66`} name="Cost" radius={[0,0,2,2]}/><Legend wrapperStyle={{fontSize:18}}/></BarChart></ResponsiveContainer></div></Card>
          <Card style={{borderLeft:`3px solid ${cG}`}}><CH title="Optimal Bundle" badge="TOP 3"/><div style={{padding:"0 16px 12px"}}>
            <div style={{fontSize:12,color:C.inkLight,marginBottom:3}}>Best 3-intervention package (75% diminishing return factor)</div>
            {interventions.bestTriple.items.map((nm:string,i:number)=><div key={i} style={{fontSize:13,fontWeight:500,padding:"1px 0"}}>{i+1}. {nm}</div>)}
            <div style={{display:"grid",gridTemplateColumns:compact?"1fr":"1fr 1fr 1fr",gap:4,marginTop:4,padding:4,background:C.surface,borderRadius:4}}>
              <div style={{textAlign:"center"}}><div style={{fontSize:11,color:C.inkLight,textTransform:"uppercase"}}>Cost</div><div style={{fontSize:16,fontWeight:300}}>{fmt(interventions.bestTriple.cost)}</div></div>
              <div style={{textAlign:"center"}}><div style={{fontSize:11,color:C.inkLight,textTransform:"uppercase"}}>Δ Rev</div><div style={{fontSize:16,fontWeight:300,color:C.pos}}>{fmt(interventions.bestTriple.delta)}</div></div>
              <div style={{textAlign:"center"}}><div style={{fontSize:11,color:C.inkLight,textTransform:"uppercase"}}>ROI</div><div style={{fontSize:16,fontWeight:300,color:interventions.bestTriple.roi>1?C.pos:C.warn}}>{interventions.bestTriple.roi.toFixed(1)}×</div></div>
            </div>
            <div style={{fontSize:11,color:C.inkLight,fontFamily:FONT.mono,marginTop:2}}>Raw sum: {fmt(interventions.bestTriple.rawD)} × 0.75 dim = {fmt(interventions.bestTriple.delta)}</div>
          </div></Card>
        </div>
        <Card><CH title="Intervention Combinations" badge="PAIRS" right="85% diminishing return"/><div style={{padding:"0 16px 12px"}}>
          <Tbl cols={["Combination","Cost","Raw Δ","Adj Δ","ROI"]} rows={interventions.combos.map((c:{nm:string;cost:number;rawD:number;delta:number;roi:number;roiCl:string})=>[
            {v:c.nm,s:{fontWeight:500,fontSize:12}},
            fmt(c.cost),
            {v:fmt(c.rawD),s:{color:C.inkLight,fontSize:12}},
            {v:fmt(c.delta),s:{color:c.delta>0?C.pos:C.neg,fontWeight:600}},
            {v:c.roi.toFixed(1)+"×",s:{color:c.roiCl,fontWeight:600}}
          ])}/>
          <div style={{fontSize:11,color:C.inkLight,marginTop:2}}>Pairs apply 85% factor to sum of individual deltas to model overlapping mechanisms.</div>
        </div></Card>
      </div>
    </div>}

    {/* ═══════════════ APM COMPARISON ═══════════════ */}
    {view==="apm"&&<div style={{display:"grid",gridTemplateColumns:sideW,gap:12}}>
      <Sidebar/>
      <div style={{display:"grid",gap:12}}>
        <Card><CH title="Alternative Payment Model Comparison" badge={`${apm.apms.length} MODELS`} right={`Best risk-adjusted: ${apm.best.nm}`}/><div style={{padding:"0 14px 8px"}}>
          <div style={{overflowX:"auto"}}><table style={{width:"100%",borderCollapse:"collapse",fontFamily:FONT.mono,fontSize:13,minWidth:550}}><thead><tr>{["Model","Type","Term","Risk","PY1 Δ","5Y Cum","P(Loss)","Sharpe","Status"].map(x=><th key={x} style={{padding:"3px 2px",textAlign:x==="Model"?"left":"right",color:C.inkLight,fontSize:12,borderBottom:`1px solid ${C.border}`}}>{x}</th>)}</tr></thead>
            <tbody>{apm.apms.map((a:{nm:string;cl:string;desc:string;type:string;term:string;risk:string;delta:number;cum5:number;downP:number;sharpe:number;rec:string},i:number)=><tr key={i} style={{borderBottom:`1px solid ${C.border}`,background:a.nm==="AHEAD"?`${cB}04`:"transparent"}}>
              <td style={{padding:"4px 2px",fontFamily:FONT.body}}><div style={{fontWeight:600,color:a.cl}}>{a.nm}</div><div style={{fontSize:11,color:C.inkLight}}>{a.desc}</div></td>
              <td style={{textAlign:"right"}}>{a.type}</td>
              <td style={{textAlign:"right"}}>{a.term}</td>
              <td style={{textAlign:"right"}}>{a.risk}</td>
              <td style={{textAlign:"right",color:a.delta>0?C.pos:C.neg,fontWeight:600}}>{fmt(a.delta)}</td>
              <td style={{textAlign:"right",fontWeight:600}}>{fmt(a.cum5)}</td>
              <td style={{textAlign:"right",color:a.downP>.4?C.neg:a.downP>.25?C.warn:C.pos}}>{(a.downP*100).toFixed(0)}%</td>
              <td style={{textAlign:"right",fontWeight:600,color:a.sharpe>0?C.pos:C.neg}}>{a.sharpe.toFixed(2)}</td>
              <td style={{textAlign:"right"}}><span style={{fontSize:12,padding:"1px 4px",borderRadius:2,background:a.rec==="Favorable"?`${C.pos}11`:a.rec==="Conditional"?`${C.warn}11`:`${C.neg}11`,color:a.rec==="Favorable"?C.pos:a.rec==="Conditional"?C.warn:C.neg,fontWeight:500}}>{a.rec}</span></td>
            </tr>)}</tbody></table></div>
        </div></Card>
        <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:12}}>
          <Card><CH title="Risk-Return Profile"/><div style={{padding:"0 14px 10px"}}><ResponsiveContainer width="100%" height={260}><ScatterChart>
            <CartesianGrid strokeDasharray="3 3" stroke={C.border}/>
            <XAxis dataKey="x" type="number" name="Downside P" tick={{fontSize:12,fill:C.inkLight,fontFamily:FONT.mono}} label={{value:"P(Loss)",fontSize:12,fill:C.inkLight,position:"bottom"}}/>
            <YAxis dataKey="y" type="number" name="PY1 Δ" tick={{fontSize:12,fill:C.inkLight,fontFamily:FONT.mono}} tickFormatter={v=>fmt(v as number)}/>
            <ReferenceLine y={0} stroke={cR} strokeDasharray="3 2"/>
            <Scatter data={apm.apms.map((a:{downP:number;delta:number;nm:string})=>({x:a.downP*100,y:a.delta,nm:a.nm}))} fill={cB}>
              {apm.apms.map((a:{cl:string},i:number)=><Cell key={i} fill={a.cl}/>)}
            </Scatter>
          </ScatterChart></ResponsiveContainer>
            <div style={{display:"flex",justifyContent:"center",gap:6,marginTop:2}}>{apm.apms.map((a:{cl:string;nm:string},i:number)=><div key={i} style={{display:"flex",alignItems:"center",gap:6}}><div style={{width:6,height:6,borderRadius:3,background:a.cl}}/><span style={{fontSize:11,color:C.inkLight}}>{a.nm}</span></div>)}</div>
          </div></Card>
          <Card><CH title="Risk-Adjusted Ranking" badge="SHARPE"/><div style={{padding:"0 14px 10px"}}><ResponsiveContainer width="100%" height={260}><BarChart data={apm.ranked} layout="vertical" margin={{left:60}}>
            <CartesianGrid strokeDasharray="3 3" stroke={C.border} horizontal={false}/>
            <XAxis type="number" tick={{fontSize:12,fill:C.inkLight,fontFamily:FONT.mono}}/>
            <YAxis type="category" dataKey="nm" tick={{fontSize:13,fill:C.ink}} width={55}/>
            <ReferenceLine x={0} stroke={cR} strokeDasharray="3 2"/>
            <Bar dataKey="sharpe" radius={[0,3,3,0]}>{apm.ranked.map((a:{sharpe:number;cl:string},i:number)=><Cell key={i} fill={a.sharpe>0?a.cl:`${C.neg}88`}/>)}</Bar>
          </BarChart></ResponsiveContainer></div></Card>
        </div>
        <Card><CH title="Stacking Opportunities" badge="COMBINABLE"/><div style={{padding:"0 16px 12px"}}>
          {apm.stackable.map((s:{a:string;b:string;note:string},i:number)=><div key={i} style={{padding:"8px 0",borderBottom:i<apm.stackable.length-1?`1px solid ${C.border}`:"none",display:"flex",justifyContent:"space-between",alignItems:"center"}}>
            <div><div style={{fontSize:13}}><span style={{fontWeight:600,color:apm.apms.find((a:{nm:string})=>a.nm===s.a)?.cl||C.ink}}>{s.a}</span><span style={{color:C.inkLight}}> + </span><span style={{fontWeight:600,color:apm.apms.find((a:{nm:string})=>a.nm===s.b)?.cl||C.ink}}>{s.b}</span></div>
              <div style={{fontSize:12,color:C.inkLight}}>{s.note}</div></div>
            <div style={{fontSize:14,fontWeight:300,color:C.pos,fontFamily:FONT.mono}}>{fmt((apm.apms.find((a:{nm:string})=>a.nm===s.a)?.delta||0)+(apm.apms.find((a:{nm:string})=>a.nm===s.b)?.delta||0))}</div>
          </div>)}
          <div style={{fontSize:12,color:cG,fontStyle:"italic",marginTop:2}}>Best combo: {apm.bestCombo.a.nm} + {apm.bestCombo.b.nm} = {fmt(apm.bestCombo.v)}</div>
        </div></Card>
        <Card style={{background:`${cB}04`,boxShadow:"none"}}><CH title="Decision Matrix"/><div style={{padding:"0 16px 12px"}}>
          <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit, minmax(130px, 1fr))",gap:3}}>
            {apm.apms.map((a:{nm:string;cl:string;delta:number;rec:string},i:number)=><div key={i} style={{padding:6,background:"#fff",borderRadius:6,borderLeft:`3px solid ${a.cl}`}}>
              <div style={{fontSize:14,fontWeight:600,color:a.cl}}>{a.nm}</div>
              <div style={{fontSize:11,color:C.inkLight,marginTop:1}}>
                {a.nm==="AHEAD"?"Highest ceiling, highest complexity. Requires dual-payer infrastructure.":
                 a.nm==="ACO REACH"?"Proven TCOC model. Lower implementation burden. Good stepping stone.":
                 a.nm==="BPCI-A"?"Surgical precision — episode-focused. Complementary to global budgets.":
                 a.nm==="MSSP"?"Conservative entry. Limited downside. Good for building VBP capability.":
                 `State-specific Medicaid track. Low risk, quality-focused.`}
              </div>
              <div style={{marginTop:3,fontSize:13,fontWeight:600,color:a.delta>0?C.pos:C.neg}}>{fmt(a.delta)} · {a.rec}</div>
            </div>)}
          </div>
        </div></Card>
      </div>
    </div>}

    {/* ═══════════════ PORTFOLIO ═══════════════ */}
    {view==="compare"&&<Card><CH title="Portfolio" badge={allH.length}/><div style={{padding:"0 16px 12px",overflowX:"auto"}}><table style={{width:"100%",borderCollapse:"collapse",fontFamily:FONT.mono,fontSize:12,minWidth:700}}><thead><tr>{["Hospital","St","Comb","Δ%","Score","Sharpe"].map(x=><th key={x} style={{padding:"2px",textAlign:x==="Hospital"?"left":"right",color:C.inkLight,fontSize:11,borderBottom:`1px solid ${C.border}`}}>{x}</th>)}</tr></thead>
      <tbody>{allH.map(x=>{const r=calcMcr(x,1),d=calcMcd(x,1);const c=r.fin+d.fin,cf=r.ffs+d.ffs;const p=portfolio.find((pp:{id:string})=>pp.id===x.id);
        return <tr key={x.id} onClick={()=>{setSelId(x.id);setUseCust(false);reset();setView("brief");}} style={{cursor:"pointer",borderBottom:`1px solid ${C.border}`}} onMouseEnter={e=>e.currentTarget.style.background=C.surface} onMouseLeave={e=>e.currentTarget.style.background="transparent"}>
        <td style={{padding:"2px",fontFamily:FONT.body,fontWeight:500}}>{x.nm}</td><td style={{textAlign:"right",color:C.inkLight}}>{x.st}</td><td style={{textAlign:"right"}}>{fmt(c)}</td><td style={{textAlign:"right",color:(c-cf)>0?C.pos:C.neg}}>{fP((c-cf)/cf)}</td><td style={{textAlign:"right"}}>{calcSc(x,r,d,pjMcr(x,5),pjMcd(x,5),runMC(x,5,0)).comp}</td><td style={{textAlign:"right"}}>{p?.sharpe.toFixed(2)||"—"}</td></tr>})}</tbody></table></div></Card>}

    {/* ═══════════════ CUSTOM ═══════════════ */}
    {view==="custom"&&<div style={{display:"grid",gridTemplateColumns:compact?"1fr":"minmax(400px,500px) 1fr",gap:12}}>
      <div style={{display:"grid",gap:4,alignContent:"start"}}>
        <Card><CH title="Import Data" badge="JSON / CSV"/><div style={{padding:"0 16px 12px"}}><DataImport onImport={hosps=>{setXH(p=>[...p,...hosps]);if(hosps.length>0){setSelId(hosps[0].id);setUseCust(false);setView("brief");reset();}}}/></div></Card>
        <Card><CH title="Manual Entry"/><div style={{padding:"0 16px 12px"}}><CustForm c={cust} sc={setCust} onUse={()=>{setUseCust(true);setView("brief");reset();}}/></div></Card>
      </div>
      <div style={{display:"grid",gap:4,alignContent:"start"}}>
        {xH.length>0&&<Card><CH title="Imported Hospitals" badge={xH.length} right={<button onClick={()=>setXH([])} style={{background:C.surface,border:`1px solid ${C.border}`,borderRadius:3,padding:"1px 4px",fontSize:11,cursor:"pointer",color:C.neg,fontFamily:FONT.mono}}>Clear All</button>}/><div style={{padding:"0 16px 12px"}}>
          {xH.map((h,i)=><div key={h.id} style={{display:"flex",justifyContent:"space-between",alignItems:"center",padding:"8px 0",borderBottom:i<xH.length-1?`1px solid ${C.border}`:"none"}}>
            <div><div style={{fontSize:14,fontWeight:500}}>{h.nm}</div><div style={{fontSize:12,color:C.inkLight,fontFamily:FONT.mono}}>{h.st} · {h.beds} beds · MCR {fmt(h.bl.ip+h.bl.op)} · MCD {fmt(h.mcd.ip+h.mcd.op)}</div></div>
            <div style={{display:"flex",gap:6}}>
              <button onClick={()=>{setSelId(h.id);setUseCust(false);setView("brief");reset();}} style={{background:C.ink,color:"#fff",border:"none",borderRadius:3,padding:"2px 5px",fontSize:11,cursor:"pointer",fontWeight:600}}>Analyze →</button>
              <button onClick={()=>setXH(p=>p.filter(x=>x.id!==h.id))} style={{background:C.surface,border:`1px solid ${C.border}`,borderRadius:3,padding:"2px 4px",fontSize:11,cursor:"pointer",color:C.neg}}>✗</button>
            </div>
          </div>)}
        </div></Card>}
        <Card style={{background:C.surface,boxShadow:"none"}}><div style={{padding:10}}>
          <div style={{fontSize:15,fontWeight:600,marginBottom:4}}>Data Sources</div>
          <div style={{fontSize:13,color:C.warn,lineHeight:1.5,marginBottom:6,padding:"6px 10px",borderRadius:6,background:`${C.warn}0A`}}>
            The 12 built-in hospitals use illustrative data calibrated to public CMS sources. For production analysis, import your hospital's actual financials via JSON or CSV.
          </div>
          <div style={{fontSize:13,color:C.inkLight,lineHeight:1.5}}>
            Medicare revenue from CMS Cost Reports (Worksheet C, Part I). VBP/HRRP/HACRP from CMS Hospital Compare. TCOC from CMS per-beneficiary spending. Medicaid revenue from state cost reports or DSH audits. HEDIS from state MCO quality reports. HCC from CMS Risk Adjustment Processing System.
          </div>
          <div style={{fontSize:15,fontWeight:600,marginTop:6,marginBottom:4}}>All 26 Engines</div>
          <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:1}}>
            {["Dual HGB","MC 200","Tornado","Breakeven","VaR/CVaR","Copula tails","Nash equilibrium","Shapley allocation","Real options","Optimal stopping","Bayesian learning","Prospect theory","Bühlmann credibility","EVPI/EVSI","Regimes","Contract design","Markov quality","Service line","Synthesis engine","Intervention combos","Stress test","APM comparison","Audit trail","Scenarios","Sensitivity TS","Peer benchmarks"].map((t,i)=><div key={i} style={{fontSize:13,color:C.inkLight}}>✓ {t}</div>)}
          </div>
        </div></Card>
      </div>
    </div>}

    {/* ═══════════════ DASHBOARD ═══════════════ */}
    {view==="dashboard"&&<div style={{display:"grid",gridTemplateColumns:sideW,gap:12}}>
      <Sidebar/>
      <div style={{display:"grid",gap:3}}>
        <Card><div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit, minmax(80px, 1fr))",borderBottom:`1px solid ${C.border}`}}>
          <Met label="Medicare" value={<DV onClick={()=>setDrill(drill==="mcr"?null:"mcr")}>{fmt(mr.fin)}</DV>} onClick={()=>setDrill(drill==="mcr"?null:"mcr")}/>
          <div style={{borderLeft:`1px solid ${C.border}`}}><Met label="Medicaid" value={<DV onClick={()=>setDrill(drill==="mcd"?null:"mcd")}>{fmt(dr.fin)}</DV>} onClick={()=>setDrill(drill==="mcd"?null:"mcd")}/></div>
          <div style={{borderLeft:`1px solid ${C.border}`,background:C.surface}}><Met label="Combined" value={<DV onClick={()=>setDrill(drill==="combined"?null:"combined")}>{fmt(cF)}</DV>} detail={fP(cPct)} trend={cPct>0?"up":"down"} onClick={()=>setDrill(drill==="combined"?null:"combined")}/></div>
          <div style={{borderLeft:`1px solid ${C.border}`}}><Met label="Score" value={<DV onClick={()=>setDrill(drill==="score"?null:"score")}>{dec.comp+""}</DV>} detail={dec.rec} onClick={()=>setDrill(drill==="score"?null:"score")}/></div>
        </div></Card>
        {drill&&<DrillPanel drill={drill} onClose={()=>setDrill(null)} mr={mr} dr={dr} hosp={modH} mp={mp} dp={dp} mc={mc}/>}
        <div style={{display:"flex",gap:0,borderBottom:`1px solid ${C.border}`,flexWrap:"wrap"}}>{([["overview","Overview"],["analytics","Analytics"],["waterfall","Waterfalls"],["projection","Projection"],["medicaid","Medicaid"]] as const).map(([k,l])=><SubT key={k} active={tab===k} onClick={()=>setTab(k)}>{l}</SubT>)}</div>
        <Fade k={tab}>

        {tab==="analytics"&&<div style={{display:"grid",gap:12}}>
          <div style={{display:"flex",gap:2,background:C.surface,borderRadius:100,padding:3,flexWrap:"wrap"}}>
            {([["sens","Sensitivity"],["risk","Risk·Tails"],["game","Game Theory"],["struct","Structural"],["opts","Options·Stop"],["behav","Behavioral"],["contract","Contract"],["regime","Regimes"]] as const).map(([k,l])=><NP key={k} active={aTab===k} onClick={()=>setATab(k)} small>{l}</NP>)}
          </div>
          <Fade k={aTab}>
          {aTab==="sens"&&<div style={{display:"grid",gap:12}}>
            <Card><CH title="Tornado" badge={sens.length}/><div style={{padding:"0 16px 10px"}}><ResponsiveContainer width="100%" height={sens.length*14+6}><BarChart data={[...sens].reverse()} layout="vertical" margin={{left:50}}><CartesianGrid strokeDasharray="3 3" stroke={C.border} horizontal={false}/><XAxis type="number" tick={{fontSize:11,fill:C.inkLight,fontFamily:FONT.mono}} tickFormatter={v=>fmt(v as number)}/><YAxis type="category" dataKey="nm" tick={{fontSize:12,fill:C.ink}} width={45}/><ReferenceLine x={0} stroke={C.ink}/><Bar dataKey="lo" fill={`${cR}99`} radius={[2,0,0,2]}/><Bar dataKey="hi" fill={`${C.pos}99`} radius={[0,2,2,0]}/></BarChart></ResponsiveContainer></div></Card>
            <Card><CH title="Breakeven"/><div style={{padding:"0 16px 10px"}}><Tbl cols={["Var","Cur","BE","Gap","✓"]} rows={be.map((b:{nm:string;cur:string;be:string;gap:string;ok:boolean})=>[{v:b.nm,s:{fontWeight:500}},b.cur,{v:b.be,s:{fontWeight:600}},b.gap,{v:b.ok?"✓":"✗",s:{color:b.ok?C.pos:C.neg}}])}/></div></Card>
            <Card><CH title="YoY"/><div style={{padding:"0 16px 10px"}}><ResponsiveContainer width="100%" height={300}><BarChart data={yoy}><CartesianGrid strokeDasharray="3 3" stroke={C.border} vertical={false}/><XAxis dataKey="cY" tick={{fontSize:12,fill:C.inkLight,fontFamily:FONT.mono}}/><YAxis tick={{fontSize:11,fill:C.inkLight,fontFamily:FONT.mono}} tickFormatter={v=>fmt(v as number)}/>
              <Bar dataKey="apaD" stackId="a" fill={cB} name="APA"/><Bar dataKey="volD" stackId="a" fill={`${cB}88`} name="Vol"/><Bar dataKey="qualD" stackId="a" fill="#48639c" name="Qual"/><Bar dataKey="mcdT" stackId="a" fill={cM} name="MCD"/><ReferenceLine y={0} stroke={C.ink}/><Legend wrapperStyle={{fontSize:18}}/></BarChart></ResponsiveContainer></div></Card>
            <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:12}}>
              <Card><CH title="Sensitivity Over Time" badge={`${yrs}Y`}/><div style={{padding:"0 16px 10px"}}><ResponsiveContainer width="100%" height={220}><LineChart data={sensTS.pyData}>
                <CartesianGrid strokeDasharray="3 3" stroke={C.border} vertical={false}/>
                <XAxis dataKey="cY" tick={{fontSize:12,fill:C.inkLight,fontFamily:FONT.mono}}/><YAxis tick={{fontSize:11,fill:C.inkLight,fontFamily:FONT.mono}} tickFormatter={v=>fmt(v as number)}/>
                {sensTS.vars.slice(0,6).map((v:string)=><Line key={v} type="monotone" dataKey={v} stroke={sensTS.vCl[v]} strokeWidth={1.5} dot={{r:1.5}} name={v}/>)}
                <Legend wrapperStyle={{fontSize:18}}/>
              </LineChart></ResponsiveContainer></div></Card>
              <Card><CH title="Sensitivity Shifts" badge="PY1→PY5"/><div style={{padding:"0 16px 10px"}}>
                {sensTS.shifts.slice(0,6).map((s:{v:string;cl:string;first:number;last:number;dir:string;chg:number},i:number)=><div key={i} style={{padding:"2px 0",borderBottom:i<5?`1px solid ${C.border}`:"none",display:"flex",justifyContent:"space-between",alignItems:"center"}}>
                  <div style={{display:"flex",alignItems:"center",gap:3}}><div style={{width:6,height:6,borderRadius:3,background:s.cl}}/><span style={{fontSize:13,fontWeight:500}}>{s.v}</span></div>
                  <div style={{display:"flex",alignItems:"center",gap:12}}>
                    <span style={{fontSize:12,fontFamily:FONT.mono,color:C.inkLight}}>{fmt(s.first)} → {fmt(s.last)}</span>
                    <span style={{fontSize:12,fontWeight:600,color:s.dir==="RISING"?C.neg:s.dir==="FALLING"?C.pos:C.inkLight,padding:"0 3px",background:s.dir==="RISING"?`${C.neg}11`:s.dir==="FALLING"?`${C.pos}11`:C.surface,borderRadius:2}}>{s.dir==="RISING"?"↑ "+Math.abs(s.chg*100).toFixed(0)+"%":s.dir==="FALLING"?"↓ "+Math.abs(s.chg*100).toFixed(0)+"%":"—"}</span>
                  </div>
                </div>)}
                <div style={{fontSize:11,color:C.inkLight,marginTop:2}}>Rising = increasing risk exposure over contract term.</div>
              </div></Card>
            </div>
          </div>}
          {aTab==="risk"&&<div style={{display:"grid",gap:12}}>
            <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:12}}>
              <Card><CH title="VaR/CVaR" badge="5%"/><div style={{padding:"0 16px 10px"}}><Tbl cols={["PY","VaR","CVaR","P(>)","ρ"]} rows={mc.map(m=>[`PY${m.py}`,{v:fmt(m.var5),s:{color:C.neg,fontWeight:600}},{v:fmt(m.cvar5),s:{color:C.neg}},(m.pA*100).toFixed(0)+"%",m.rho.toFixed(2)])}/></div></Card>
              <Card><CH title="Bühlmann" badge={`Z=${cred.z.toFixed(2)}`}/><div style={{padding:"0 16px 10px"}}><ResponsiveContainer width="100%" height={85}><LineChart data={cred.adj}><CartesianGrid strokeDasharray="3 3" stroke={C.border} vertical={false}/><XAxis dataKey="cY" tick={{fontSize:12,fill:C.inkLight,fontFamily:FONT.mono}}/><YAxis tick={{fontSize:11,fill:C.inkLight,fontFamily:FONT.mono}} tickFormatter={v=>fmt(v as number)}/><Line type="monotone" dataKey="raw" stroke={cB} strokeWidth={1} dot={{r:1}}/><Line type="monotone" dataKey="cred" stroke={cG} strokeWidth={2} dot={{r:1,fill:cG}}/><ReferenceLine y={0} stroke={cR} strokeDasharray="3 2"/></LineChart></ResponsiveContainer></div></Card>
            </div>
            <Card><CH title="Copula Tail Dependence"/><div style={{padding:"0 16px 10px"}}><Tbl cols={["PY","λ_lower","λ_upper","ρ","Diversification"]} rows={(copula.years||[]).map((c:{cY:number;lL:number;lU:number;rho:number;div:string})=>[c.cY,{v:c.lL.toFixed(2),s:{color:c.lL>.5?C.neg:C.pos,fontWeight:600}},c.lU.toFixed(2),c.rho.toFixed(2),{v:c.div,s:{color:c.div==="HIGH"?C.pos:c.div==="MED"?C.warn:C.neg,fontWeight:600}}])}/></div></Card>
          </div>}
          {aTab==="game"&&<div style={{display:"grid",gap:12}}>
            <Card><CH title="Nash Equilibrium" right={`${nash.stable}/${nash.results.length} stable`}/><div style={{padding:"0 16px 10px"}}><Tbl cols={["Hospital","St","Alone","w/Mkt","Equil"]} rows={nash.results.map((r:{nm:string;st:string;alone:number;withAll:number;eq:string})=>[{v:r.nm,s:{fontWeight:500,fontSize:12}},r.st,{v:fmt(r.alone),s:{color:r.alone>0?C.pos:C.neg}},{v:fmt(r.withAll),s:{color:r.withAll>0?C.pos:C.neg,fontWeight:600}},{v:r.eq,s:{color:r.eq==="Dominant"?C.pos:r.eq==="Contingent"?C.warn:C.neg,fontWeight:600}}])}/></div></Card>
            <Card><CH title="Shapley Allocation" badge="FAIR VALUE"/><div style={{padding:"0 16px 10px"}}><Tbl cols={["Hospital","St","Shapley","Naive","Diff"]} rows={shapley.map((s:{nm:string;st:string;shapley:number;naive:number;diff:number})=>[{v:s.nm,s:{fontWeight:500,fontSize:12}},s.st,{v:fmt(s.shapley),s:{fontWeight:600,color:s.shapley>0?C.pos:C.neg}},fmt(s.naive),{v:fmt(s.diff),s:{color:s.diff>0?C.pos:s.diff<0?C.neg:C.inkLight}}])}/></div></Card>
            <Card><CH title="Efficient Frontier"/><div style={{padding:"0 16px 10px"}}><ResponsiveContainer width="100%" height={230}><ScatterChart><CartesianGrid strokeDasharray="3 3" stroke={C.border}/><XAxis dataKey="risk" type="number" tick={{fontSize:12,fill:C.inkLight,fontFamily:FONT.mono}}/><YAxis dataKey="ret" type="number" tick={{fontSize:12,fill:C.inkLight,fontFamily:FONT.mono}}/><Scatter data={portfolio}>{portfolio.map((p:{ret:number},i:number)=><Cell key={i} fill={p.ret>0?C.pos:C.neg}/>)}</Scatter></ScatterChart></ResponsiveContainer></div></Card>
          </div>}
          {aTab==="struct"&&<div style={{display:"grid",gap:12}}>
            <Card><CH title="Service Line"/><div style={{padding:"0 16px 10px"}}><ResponsiveContainer width="100%" height={svc.length*15+6}><BarChart data={svc} layout="vertical" margin={{left:50}}><CartesianGrid strokeDasharray="3 3" stroke={C.border} horizontal={false}/><XAxis type="number" tick={{fontSize:11,fill:C.inkLight,fontFamily:FONT.mono}} tickFormatter={v=>fmt(v as number)}/><YAxis type="category" dataKey="nm" tick={{fontSize:12,fill:C.ink}} width={45}/><ReferenceLine x={0} stroke={C.ink}/><Bar dataKey="v">{svc.map((s:{v:number},i:number)=><Cell key={i} fill={s.v>0?`${C.pos}CC`:`${cR}99`}/>)}</Bar></BarChart></ResponsiveContainer></div></Card>
            <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:12}}>
              <Card><CH title="Cohort Timing"/><div style={{padding:"0 16px 10px"}}><ResponsiveContainer width="100%" height={85}><LineChart><CartesianGrid strokeDasharray="3 3" stroke={C.border} vertical={false}/><XAxis dataKey="py" type="number" domain={[1,5]} tick={{fontSize:12,fill:C.inkLight,fontFamily:FONT.mono}}/><YAxis tick={{fontSize:11,fill:C.inkLight,fontFamily:FONT.mono}} tickFormatter={v=>fP(v as number)}/><ReferenceLine y={0} stroke={cR} strokeDasharray="3 2"/>{timing.map((t:{co:number;res:{py:number;pct:number}[]},i:number)=><Line key={i} data={t.res} dataKey="pct" type="monotone" stroke={[cB,cM,cT][i]} strokeWidth={2} dot={{r:1}} name={`C${t.co}`}/>)}<Legend wrapperStyle={{fontSize:18}}/></LineChart></ResponsiveContainer></div></Card>
              <Card><CH title="Markov Quality"/><div style={{padding:"0 16px 10px"}}><table style={{width:"100%",borderCollapse:"collapse",fontFamily:FONT.mono,fontSize:12}}><thead><tr><th style={{textAlign:"left",color:C.inkLight,fontSize:11}}>→</th>{["Q1","Q2","Q3","Q4"].map(q=><th key={q} style={{textAlign:"right",color:C.inkLight,fontSize:11}}>{q}</th>)}</tr></thead><tbody>{markov.T.map((row:number[],i:number)=><tr key={i}><td style={{fontWeight:500}}>Q{i+1}</td>{row.map((p:number,j:number)=><td key={j} style={{textAlign:"right",color:i===j?C.pos:C.inkLight}}>{(p*100).toFixed(0)}%</td>)}</tr>)}</tbody></table>
                <div style={{fontSize:11,color:C.inkLight,marginTop:1}}>VBP: Q{markov.vbpQ+1} · Jump value: {fmt(markov.qImp)}</div></div></Card>
            </div>
            <Card><CH title="EVPI/EVSI" right={`EVPI: ${fmt(evpi.evpi)}`}/><div style={{padding:"0 16px 10px"}}><Tbl cols={["Source","EVSI","Investment"]} rows={evpi.evsi.slice(0,4).map((e:{nm:string;val:number;inv:string})=>[{v:e.nm,s:{fontWeight:500,fontSize:12}},{v:fmt(e.val),s:{fontWeight:600}},{v:e.inv,s:{fontSize:11}}])}/></div></Card>
          </div>}
          {aTab==="opts"&&<div style={{display:"grid",gap:12}}>
            <Card><CH title="Real Options" badge="B-S" right={opts.rec}/><div style={{padding:"0 16px 10px"}}>
              <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit, minmax(65px, 1fr))",gap:3,marginBottom:3}}>{[{l:"NPV",v:fmt(opts.npv),cl:opts.npv>0?C.pos:C.neg},{l:"Time",v:fmt(opts.timeV),cl:cT},{l:"Strategic",v:fmt(opts.stratV),cl:cG},{l:"Learning",v:fmt(opts.learnV),cl:cO},{l:"Total",v:fmt(opts.optVal),cl:opts.optVal>0?C.pos:C.neg}].map((m,i)=><div key={i} style={{padding:3,background:C.surface,borderRadius:3,textAlign:"center"}}><div style={{fontSize:11,color:C.inkLight,textTransform:"uppercase"}}>{m.l}</div><div style={{fontSize:18,fontWeight:300,color:m.cl}}>{m.v}</div></div>)}</div>
            </div></Card>
            <Card><CH title="Optimal Stopping" badge="BELLMAN" right={optStop.optPolicy}/><div style={{padding:"0 16px 10px"}}><Tbl cols={["PY","Exp Δ","Cum Δ","Cont V","Continue?"]} rows={optStop.frontier.map(f=>[`PY${f.py}`,{v:fmt(f.expD),s:{color:f.expD>0?C.pos:C.neg}},{v:fmt(f.cumD),s:{fontWeight:600}},fmt(f.contV),{v:f.shouldCont?"YES":"EXIT",s:{color:f.shouldCont?C.pos:C.neg,fontWeight:600}}])}/></div></Card>
          </div>}
          {aTab==="behav"&&<div style={{display:"grid",gap:12}}>
            <Card><CH title="Prospect Theory" badge={`λ=${prospect.lossAv}`}/><div style={{padding:"0 16px 10px"}}><Tbl cols={["PY","EV","Prospect V","PT/EV","Regret"]} rows={(prospect.years||[]).map((y:{cY:number;ev:number;pv:number;ratio:number;regretP:number})=>[y.cY,{v:fmt(y.ev),s:{color:y.ev>0?C.pos:C.neg}},{v:fmt(y.pv),s:{color:y.pv>0?C.pos:C.neg,fontWeight:600}},{v:y.ratio.toFixed(2)+"×",s:{color:y.ratio<.85?C.neg:C.warn}},(y.regretP*100).toFixed(0)+"%"])}/></div></Card>
            <Card><CH title="Bayesian Learning" badge="POSTERIOR"/><div style={{padding:"0 16px 10px"}}><ResponsiveContainer width="100%" height={170}><ComposedChart data={bayesian.years||[]}><CartesianGrid strokeDasharray="3 3" stroke={C.border} vertical={false}/><XAxis dataKey="cY" tick={{fontSize:12,fill:C.inkLight,fontFamily:FONT.mono}}/><YAxis tick={{fontSize:11,fill:C.inkLight,fontFamily:FONT.mono}} tickFormatter={v=>fmt(v as number)}/>
              <Area type="monotone" dataKey="ci95l" stackId="ci" fill="transparent" stroke="transparent"/><Area type="monotone" dataKey="ci95u" fill={`${cT}12`} stroke="transparent"/>
              <Line type="monotone" dataKey="postMu" stroke={cT} strokeWidth={2} dot={{r:2,fill:cT}}/><Line type="monotone" dataKey="obs" stroke={cO} strokeWidth={0} dot={{r:3,fill:cO}}/><ReferenceLine y={0} stroke={cR} strokeDasharray="3 2"/></ComposedChart></ResponsiveContainer>
              <div style={{fontSize:11,color:C.inkLight}}>σ reduction: {((bayesian.totalRed||0)*100).toFixed(0)}% over {bayesian.years?.length||0} years</div>
            </div></Card>
          </div>}
          {aTab==="contract"&&<Card><CH title="Contract Design" badge="REVERSE SOLVER" right={contract.gap<=0?"Favorable":`Gap: ${fmt(contract.gap)}`}/><div style={{padding:"0 16px 10px"}}><Tbl cols={["Term","Current","Needed","✓","Impact"]} rows={contract.terms.map((t:{nm:string;cur:string;need:string;ok:boolean;imp:number})=>[{v:t.nm,s:{fontWeight:500}},t.cur,{v:t.need,s:{fontWeight:600}},{v:t.ok?"✓":"✗",s:{color:t.ok?C.pos:C.neg}},fmt(t.imp)])}/></div></Card>}
          {aTab==="regime"&&<Card><CH title="Regime Scenarios" right={`EV: ${fmt(regimes.wEV)}`}/><div style={{padding:"0 16px 10px"}}>
            <Tbl cols={["Regime","Prob","Δ","Δ%"]} rows={regimes.results.map((r:{nm:string;prob:number;delta:number;pct:number})=>[{v:r.nm,s:{fontWeight:500}},(r.prob*100).toFixed(0)+"%",{v:r.delta?fmt(r.delta):"FFS",s:{color:r.delta>0?C.pos:r.delta<0?C.neg:C.inkLight,fontWeight:600}},r.pct?fP(r.pct):"—"])}/>
            <ResponsiveContainer width="100%" height={85}><BarChart data={regimes.results.filter((r:{delta:number})=>r.delta)}><CartesianGrid strokeDasharray="3 3" stroke={C.border} vertical={false}/><XAxis dataKey="nm" tick={{fontSize:11,fill:C.inkLight}} interval={0}/><YAxis tick={{fontSize:11,fill:C.inkLight,fontFamily:FONT.mono}} tickFormatter={v=>fmt(v as number)}/><ReferenceLine y={0} stroke={cR} strokeDasharray="3 2"/><Bar dataKey="delta">{regimes.results.filter((r:{delta:number})=>r.delta).map((r:{delta:number},i:number)=><Cell key={i} fill={r.delta>0?`${C.pos}CC`:`${C.neg}99`}/>)}</Bar></BarChart></ResponsiveContainer>
          </div></Card>}
          </Fade>
        </div>}

        {tab==="overview"&&<div style={{display:"grid",gap:12}}>
          <Card><CH title="Decision Scorecard" badge={dec.comp} right={<span style={{color:dec.recCl,fontWeight:600}}>{dec.rec}</span>}/><div style={{padding:"0 16px 10px"}}>
            <ResponsiveContainer width="100%" height={dec.factors.length*16+4}><BarChart data={dec.factors} layout="vertical" margin={{left:45}}>
              <CartesianGrid strokeDasharray="3 3" stroke={C.border} horizontal={false}/>
              <XAxis type="number" domain={[0,10]} tick={{fontSize:11,fill:C.inkLight,fontFamily:FONT.mono}} ticks={[0,2,4,6,8,10]}/>
              <YAxis type="category" dataKey="nm" tick={{fontSize:12,fill:C.ink}} width={40}/>
              <Bar dataKey="sc" radius={[0,3,3,0]}>{dec.factors.map((f:{sc:number},i:number)=><Cell key={i} fill={f.sc>=7?`${C.pos}CC`:f.sc>=5?`${C.warn}CC`:`${C.neg}99`}/>)}</Bar>
            </BarChart></ResponsiveContainer>
            <div style={{display:"flex",justifyContent:"space-between",padding:"2px 0",fontSize:11,color:C.inkLight,fontFamily:FONT.mono,borderTop:`1px solid ${C.border}`}}>
              {dec.factors.map((f:{nm:string;det:string},i:number)=><span key={i}>{f.nm}: {f.det}</span>)}
            </div>
          </div></Card>
          <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:12}}>
            <Card><CH title="MC Distribution" badge="PY1" right={<DV onClick={()=>setDrill(drill==="mc"?null:"mc")}>{((mc[0]?.pA||0)*100).toFixed(0)}% P(&gt;FFS)</DV>}/><div style={{padding:"0 16px 10px"}}>{(()=>{
              const raw=mc[0]?.rawC||[];if(!raw.length)return <div style={{fontSize:12,color:C.inkLight}}>No MC data</div>;
              const mn=Math.min(...raw),mx=Math.max(...raw),bins=20,bw=(mx-mn)/bins||1;
              const hist=Array(bins).fill(0) as number[];raw.forEach(v=>{const b=Math.min(bins-1,Math.floor((v-mn)/bw));hist[b]++;});
              const hData=hist.map((c,i)=>({x:mn+i*bw+bw/2,c,pos:mn+i*bw+bw/2>0}));
              return <ResponsiveContainer width="100%" height={300}><BarChart data={hData}>
                <CartesianGrid strokeDasharray="3 3" stroke={C.border} vertical={false}/>
                <XAxis dataKey="x" tick={{fontSize:11,fill:C.inkLight,fontFamily:FONT.mono}} tickFormatter={v=>fmt(v as number)}/>
                <YAxis tick={{fontSize:11,fill:C.inkLight,fontFamily:FONT.mono}}/>
                <ReferenceLine x={0} stroke={cR} strokeWidth={2} label={{value:"FFS",fontSize:11,fill:cR}}/>
                <Bar dataKey="c" radius={[1,1,0,0]}>{hData.map((d,i)=><Cell key={i} fill={d.pos?`${C.pos}88`:`${C.neg}77`}/>)}</Bar>
              </BarChart></ResponsiveContainer>})()}</div></Card>
            <Card><CH title="Payer Breakdown"/><div style={{padding:"0 16px 10px"}}><ResponsiveContainer width="100%" height={300}><BarChart data={[{n:"Base",m:mr.wT,d:dr.wT},{n:"Trend",m:mr.pV,d:dr.pV},{n:"Adj",m:mr.sra+mr.qD+mr.eff,d:dr.sdA+dr.hA},{n:"Final",m:mr.fin,d:dr.fin}]}><CartesianGrid strokeDasharray="3 3" stroke={C.border} vertical={false}/><XAxis dataKey="n" tick={{fontSize:12,fill:C.inkLight}}/><YAxis tick={{fontSize:11,fill:C.inkLight,fontFamily:FONT.mono}} tickFormatter={v=>fmt(v as number)}/>
              <Bar dataKey="m" stackId="a" fill={cB} name="MCR"/><Bar dataKey="d" stackId="a" fill={cM} name="MCD" radius={[2,2,0,0]}/><Legend wrapperStyle={{fontSize:18}}/></BarChart></ResponsiveContainer></div></Card>
          </div>
        </div>}
        {tab==="waterfall"&&<div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:4}}><Card><CH title="MCR"/><div style={{padding:"0 8px 3px"}}><WF data={mr.wf}/></div></Card><Card><CH title="MCD" badge={hosp.st}/><div style={{padding:"0 8px 3px"}}><WF data={dr.wf} color={cM}/></div></Card></div>}
        {tab==="projection"&&<div style={{display:"grid",gap:12}}>
          <Card><CH title={`${yrs}Y Projection`} badge="FAN CHART"/><div style={{padding:"0 16px 10px"}}><ResponsiveContainer width="100%" height={250}><ComposedChart data={mc}>
            <CartesianGrid strokeDasharray="3 3" stroke={C.border} vertical={false}/>
            <XAxis dataKey="cY" tick={{fontSize:12,fill:C.inkLight,fontFamily:FONT.mono}}/><YAxis tick={{fontSize:11,fill:C.inkLight,fontFamily:FONT.mono}} tickFormatter={v=>fmt(v as number)}/>
            <Area type="monotone" dataKey="cp10" stackId="f1" fill="transparent" stroke="transparent"/>
            <Area type="monotone" dataKey="cp90" fill={`${cB}06`} stroke={`${cB}20`} strokeWidth={.5}/>
            <Area type="monotone" dataKey="cp25" stackId="f2" fill="transparent" stroke="transparent"/>
            <Area type="monotone" dataKey="cp75" fill={`${cB}12`} stroke={`${cB}30`} strokeWidth={.5}/>
            <Line type="monotone" dataKey="cp50" stroke={cB} strokeWidth={2} dot={{r:2,fill:cB}} name="Median"/>
            <Line type="monotone" dataKey="cffs" stroke="#9CA3AF" strokeWidth={1.5} strokeDasharray="5 3" dot={false} name="FFS"/>
            <Legend wrapperStyle={{fontSize:18}}/>
          </ComposedChart></ResponsiveContainer></div></Card>
          <Card><CH title="Detailed Projection"/><div style={{padding:"0 16px 10px"}}><Tbl cols={["PY","CY","MCR","MCD","Combined","Δ","Δ%","P(>)","VaR","CVaR"]} rows={mp.map((m,i)=>{const d=dp[i],cc=m.fin+d.fin,cf=m.ffs+d.ffs,dd=cc-cf;return[`PY${m.py}`,m.cY,fmt(m.fin),{v:fmt(d.fin),s:{color:cM}},{v:fmt(cc),s:{fontWeight:600}},{v:fmt(dd),s:{color:dd>0?C.pos:C.neg,fontWeight:600}},{v:fP(dd/cf),s:{color:dd>0?C.pos:C.neg}},((mc[i]?.pA||0)*100).toFixed(0)+"%",{v:fmt(mc[i]?.var5||0),s:{color:C.neg,fontSize:12}},{v:fmt(mc[i]?.cvar5||0),s:{color:C.neg,fontSize:12}}];})}/></div></Card>
        </div>}
        {tab==="medicaid"&&<div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:12}}>
          <Card><CH title="State" badge={hosp.st}/><div style={{padding:"0 10px 3px"}}>{[{l:"Method",v:sp.md},{l:"FMAP",v:fP(sp.fmap)},{l:"MC%",v:fP(sp.mcPct)},{l:"UPL",v:sp.upl+"×"}].map((r,i)=><div key={i} style={{display:"flex",justifyContent:"space-between",padding:"1px 0",fontSize:13}}><span style={{color:C.inkLight}}>{r.l}</span><span style={{fontFamily:FONT.mono}}>{r.v}</span></div>)}</div></Card>
          <Card><CH title="HEDIS"/><div style={{padding:"0 10px 3px"}}>{[{l:"Pre",v:hosp.mcd.hedis.pre},{l:"ED",v:hosp.mcd.hedis.ed},{l:"Dia",v:hosp.mcd.hedis.dia},{l:"FU",v:hosp.mcd.hedis.fu}].map((r,i)=><div key={i} style={{padding:"1px 0"}}><div style={{display:"flex",justifyContent:"space-between",fontSize:13}}><span>{r.l}</span><span style={{fontFamily:FONT.mono,color:r.v>=.75?C.pos:C.neg}}>{(r.v*100).toFixed(0)}th</span></div><div style={{height:2,background:C.surface,borderRadius:100,overflow:"hidden"}}><div style={{height:"100%",width:`${r.v*100}%`,background:r.v>=.75?C.pos:C.neg}}/></div></div>)}</div></Card>
        </div>}
        </Fade>
      </div>
    </div>}
    </Fade>
  </div>);
}
