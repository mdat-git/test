import React from "react";
import { useMemo, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Select, SelectTrigger, SelectValue, SelectContent, SelectItem } from "@/components/ui/select";
import { Badge } from "@/components/ui/badge";
import { Slider } from "@/components/ui/slider";
import { Label } from "@/components/ui/label";
import { Calendar } from "@/components/ui/calendar";
import { Download, Filter, Search, RefreshCw, TrendingUp, Clock, Activity, Zap, Users, ShieldCheck, Map, BarChart3, Layers, Gauge, Database } from "lucide-react";
import { ResponsiveContainer, LineChart, Line, AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, BarChart, Bar, Legend, PieChart, Pie, Cell } from "recharts";
import { motion } from "framer-motion";

// --- Mock Data (replace with real API data) ---
const kpi = {
  saidi: 42.1,
  saifi: 0.87,
  caidi: 48.6,
  cmiSavedPct: 12.3,
  validationsPerDay: 386,
  dangerousMissRate: 0.3,
  etrMAE: 28.4, // minutes
};

const timeSeries = Array.from({ length: 26 }, (_, i) => ({
  date: `2025-W${(i + 1).toString().padStart(2, "0")}`,
  saidi: 20 + Math.random() * 40,
  saifi: 0.5 + Math.random() * 0.8,
  cmi: 2.5 + Math.random() * 4.0,
  validations: 200 + Math.round(Math.random() * 250),
  etr_mae: 15 + Math.random() * 35,
}));

const causeBreakdown = [
  { cause: "OH Equipment", pct: 32 },
  { cause: "UG Equipment", pct: 18 },
  { cause: "Vegetation", pct: 22 },
  { cause: "Weather", pct: 9 },
  { cause: "Animals", pct: 7 },
  { cause: "3rd Party", pct: 6 },
  { cause: "Other", pct: 6 },
];

const districtPerf = Array.from({ length: 10 }, (_, i) => ({
  district: `D${i + 1}`,
  saidi: Math.round(20 + Math.random() * 60),
  saifi: +(0.3 + Math.random()).toFixed(2),
  cmiSavedPct: +(5 + Math.random() * 20).toFixed(1),
}));

// --- Small helper components ---
const KPI = ({ icon: Icon, label, value, suffix, help }: any) => (
  <Card className="rounded-2xl shadow-sm">
    <CardContent className="p-4 flex items-center gap-3">
      <div className="p-3 rounded-xl bg-muted"><Icon className="w-5 h-5" /></div>
      <div className="flex-1">
        <div className="text-xs text-muted-foreground">{label}</div>
        <div className="text-2xl font-semibold tracking-tight">{value}{suffix}</div>
      </div>
      {help && <Badge variant="secondary" className="hidden md:inline">{help}</Badge>}
    </CardContent>
  </Card>
);

const Panel = ({ title, icon: Icon, children, actions }: any) => (
  <Card className="rounded-2xl shadow-sm">
    <CardHeader className="pb-3 flex-row items-center justify-between">
      <div className="flex items-center gap-2">
        <Icon className="w-5 h-5" />
        <CardTitle className="text-base font-semibold">{title}</CardTitle>
      </div>
      {actions}
    </CardHeader>
    <CardContent className="pt-0">{children}</CardContent>
  </Card>
);

// --- Filters ---
function FilterBar() {
  const [district, setDistrict] = useState("All");
  const [feeder, setFeeder] = useState("All");
  const [classif, setClassif] = useState("All");
  return (
    <div className="grid grid-cols-1 md:grid-cols-5 gap-3">
      <Select value={district} onValueChange={setDistrict}>
        <SelectTrigger className="rounded-xl"><SelectValue placeholder="District" /></SelectTrigger>
        <SelectContent>
          <SelectItem value="All">All Districts</SelectItem>
          {Array.from({ length: 12 }, (_, i) => (
            <SelectItem key={i} value={`D${i + 1}`}>{`D${i + 1}`}</SelectItem>
          ))}
        </SelectContent>
      </Select>
      <Select value={feeder} onValueChange={setFeeder}>
        <SelectTrigger className="rounded-xl"><SelectValue placeholder="Feeder" /></SelectTrigger>
        <SelectContent>
          <SelectItem value="All">All Feeders</SelectItem>
          {Array.from({ length: 8 }, (_, i) => (
            <SelectItem key={i} value={`FDR-${i + 1}`}>{`FDR-${i + 1}`}</SelectItem>
          ))}
        </SelectContent>
      </Select>
      <Select value={classif} onValueChange={setClassif}>
        <SelectTrigger className="rounded-xl"><SelectValue placeholder="Incident Class" /></SelectTrigger>
        <SelectContent>
          {['All','Single-Line','Multi-Step','Planned','Storm','Major Event'].map(c => (
            <SelectItem key={c} value={c}>{c}</SelectItem>
          ))}
        </SelectContent>
      </Select>
      <Input className="rounded-xl" placeholder="Search incident, device, note…" />
      <div className="flex gap-2">
        <Button variant="outline" className="rounded-xl w-full md:w-auto"><Filter className="w-4 h-4 mr-2"/>Advanced</Button>
        <Button className="rounded-xl w-full md:w-auto"><Search className="w-4 h-4 mr-2"/>Query</Button>
      </div>
    </div>
  );
}

// --- Charts ---
function TrendChart() {
  return (
    <div className="h-64">
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart data={timeSeries} margin={{ top: 8, right: 12, left: 0, bottom: 0 }}>
          <defs>
            <linearGradient id="g1" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopOpacity={0.35} />
              <stop offset="100%" stopOpacity={0} />
            </linearGradient>
          </defs>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="date" tick={{ fontSize: 12 }} />
          <YAxis yAxisId="left" tick={{ fontSize: 12 }} />
          <YAxis yAxisId="right" orientation="right" tick={{ fontSize: 12 }} />
          <Tooltip />
          <Legend />
          <Area yAxisId="left" type="monotone" dataKey="saidi" name="SAIDI (min)" strokeWidth={2} fillOpacity={1} fill="url(#g1)" />
          <Line yAxisId="left" type="monotone" dataKey="saifi" name="SAIFI" strokeWidth={2} dot={false} />
          <Line yAxisId="right" type="monotone" dataKey="etr_mae" name="ETR MAE (min)" strokeDasharray="5 3" strokeWidth={2} dot={false} />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}

function DistrictBars() {
  return (
    <div className="h-64">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={districtPerf}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="district" tick={{ fontSize: 12 }} />
          <YAxis tick={{ fontSize: 12 }} />
          <Tooltip />
          <Legend />
          <Bar dataKey="saidi" name="SAIDI" />
          <Bar dataKey="saifi" name="SAIFI" />
          <Bar dataKey="cmiSavedPct" name="CMI Saved %" />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

function CausePie() {
  return (
    <div className="h-64">
      <ResponsiveContainer width="100%" height="100%">
        <PieChart>
          <Tooltip />
          <Legend />
          <Pie data={causeBreakdown} dataKey="pct" nameKey="cause" innerRadius={50} outerRadius={90} />
        </PieChart>
      </ResponsiveContainer>
    </div>
  );
}

// --- Main Component ---
export default function OMSReliabilityDashboard() {
  return (
    <div className="min-h-screen w-full bg-background text-foreground">
      <motion.header initial={{ opacity: 0, y: -10 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.4 }}
        className="sticky top-0 z-10 backdrop-blur supports-[backdrop-filter]:bg-background/75 border-b">
        <div className="max-w-7xl mx-auto px-4 py-3 flex items-center gap-3">
          <div className="p-2 rounded-xl bg-muted"><Layers className="w-5 h-5" /></div>
          <h1 className="font-semibold tracking-tight text-lg">NOVA · OMS Reliability Dashboard</h1>
          <Badge variant="secondary" className="ml-auto">v1</Badge>
          <Button variant="outline" size="sm" className="rounded-xl"><RefreshCw className="w-4 h-4 mr-2"/>Refresh</Button>
          <Button size="sm" className="rounded-xl"><Download className="w-4 h-4 mr-2"/>Export</Button>
        </div>
      </motion.header>

      <main className="max-w-7xl mx-auto px-4 py-5 space-y-5">
        {/* Filters */}
        <Panel title="Filters" icon={Filter} actions={<Badge variant="outline">Last 12 months</Badge>}>
          <FilterBar />
        </Panel>

        {/* KPI Row */}
        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
          <KPI icon={TrendingUp} label="SAIDI (min)" value={kpi.saidi} help="IEEE 1366" />
          <KPI icon={Activity} label="SAIFI" value={kpi.saifi} />
          <KPI icon={Clock} label="CAIDI (min)" value={kpi.caidi} />
          <KPI icon={Zap} label="CMI Saved vs Baseline" value={kpi.cmiSavedPct} suffix="%" />
        </div>

        {/* Second KPI Row */}
        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
          <KPI icon={Gauge} label="Validations / Day" value={kpi.validationsPerDay} />
          <KPI icon={ShieldCheck} label="Dangerous Miss Rate" value={kpi.dangerousMissRate} suffix="%" />
          <KPI icon={BarChart3} label="ETR MAE (min)" value={kpi.etrMAE} />
        </div>

        <Tabs defaultValue="exec" className="w-full">
          <TabsList className="grid w-full grid-cols-4 rounded-xl">
            <TabsTrigger value="exec">Executive</TabsTrigger>
            <TabsTrigger value="ops">Validation Ops</TabsTrigger>
            <TabsTrigger value="etr">ETR Quality</TabsTrigger>
            <TabsTrigger value="cause">Cause & Classification</TabsTrigger>
          </TabsList>

          {/* Executive */}
          <TabsContent value="exec" className="space-y-4">
            <Panel title="Reliability Trends" icon={Activity}>
              <TrendChart />
            </Panel>
            <div className="grid gap-4 md:grid-cols-2">
              <Panel title="District Performance" icon={Map}>
                <DistrictBars />
              </Panel>
              <Panel title="Cause Mix" icon={Database}>
                <CausePie />
              </Panel>
            </div>
          </TabsContent>

          {/* Validation Ops */}
          <TabsContent value="ops" className="space-y-4">
            <Panel title="Validation Throughput" icon={Users}>
              <div className="h-64">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={timeSeries}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="date" tick={{ fontSize: 12 }} />
                    <YAxis tick={{ fontSize: 12 }} />
                    <Tooltip />
                    <Bar dataKey="validations" name="Validations / Day" />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </Panel>
            <Panel title="Work Mix & Savings" icon={Gauge}>
              <div className="grid md:grid-cols-2 gap-3">
                <div>
                  <Label>Auto-approve threshold</Label>
                  <Slider defaultValue={[80]} step={1} min={50} max={99} className="mt-2" />
                  <p className="text-sm text-muted-foreground mt-2">Tune threshold to balance workload saved vs. Dangerous Miss rate.</p>
                </div>
                <div className="h-40">
                  <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={timeSeries}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis dataKey="date" hide />
                      <YAxis />
                      <Tooltip />
                      <Line type="monotone" dataKey="cmi" name="CMI (M)" dot={false} />
                    </LineChart>
                  </ResponsiveContainer>
                </div>
              </div>
            </Panel>
          </TabsContent>

          {/* ETR Quality */}
          <TabsContent value="etr" className="space-y-4">
            <Panel title="ETR Error Over Time" icon={Clock}>
              <div className="h-64">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={timeSeries}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="date" tick={{ fontSize: 12 }} />
                    <YAxis tick={{ fontSize: 12 }} />
                    <Tooltip />
                    <Legend />
                    <Line type="monotone" dataKey="etr_mae" name="MAE (min)" dot={false} />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </Panel>
            <Panel title="Top ETR Failure Patterns (NLP)" icon={Layers}>
              <ul className="text-sm grid md:grid-cols-2 gap-2">
                {["Disable ETR recalculation @ <place>", "Remove ETR for <place> (MAN/SYS)", "Inspection ETR set then cleared", "Conflicting Followup: CREW_ACTION vs FOLLOWUP", "CGI_HISMGR archived → reopened"].map((t, i) => (
                  <li key={i} className="p-2 rounded-lg bg-muted">{t}</li>
                ))}
              </ul>
            </Panel>
          </TabsContent>

          {/* Cause & Classification */}
          <TabsContent value="cause" className="space-y-4">
            <Panel title="Causes by District" icon={BarChart3}>
              <div className="h-64">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={districtPerf}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="district" />
                    <YAxis />
                    <Tooltip />
                    <Legend />
                    <Bar dataKey="cmiSavedPct" name="% Low-Impact Auto-Validated" />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </Panel>
            <Panel title="Cause Distribution" icon={Database}>
              <CausePie />
            </Panel>
          </TabsContent>
        </Tabs>
      </main>
    </div>
  );
}
