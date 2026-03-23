import React, { useEffect, useState, lazy, Suspense, useContext } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert';
import { Progress } from '@/components/ui/progress';
import {
  TrendingUp, TrendingDown, Package, Clock, Truck, AlertTriangle,
  MapPin, Activity, ArrowUp, ArrowDown, Sparkles, BarChart3,
  Calendar, Users, Factory, Info, RefreshCw, ArrowRight, Loader2, Bot, CheckCircle
} from 'lucide-react';
import { ElenaKPIs, TrendingProduct, SupplierMetrics, WarehouseDetail, HomepageData } from '@/types';
import {
  LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid,
  Tooltip as RechartsTooltip, Legend, ResponsiveContainer, Cell
} from 'recharts';
import { cn } from '@/lib/utils';
import { format } from 'date-fns';
import { FloatingGenieContext } from '@/App';

// Lazy load the map component
const LeafletWarehouseMap = lazy(() => import('@/components/LeafletWarehouseMap'));

interface HomepageProps {
  kpis: ElenaKPIs;
  kpisLoading: boolean;
  otprLoading: boolean;
  turnoverLoading: boolean;
  expeditedLoading: boolean;
  daysOfStockLoading: boolean;
  refreshOTPR: () => void;
  refreshInventoryTurnover: () => void;
  refreshExpeditedCosts: () => void;
  refreshDaysOfStock: () => void;
  refreshData: () => void;
  setActiveTab: (tab: string) => void;
  criticalCount?: number;
  warningCount?: number;
}

const Homepage: React.FC<HomepageProps> = ({
  kpis,
  kpisLoading,
  otprLoading,
  turnoverLoading,
  expeditedLoading,
  daysOfStockLoading,
  refreshOTPR,
  refreshInventoryTurnover,
  refreshExpeditedCosts,
  refreshDaysOfStock,
  refreshData,
  setActiveTab,
  criticalCount: propCriticalCount,
  warningCount: propWarningCount
}) => {
  const floatingGenieRef = useContext(FloatingGenieContext);
  const [dailySummary, setDailySummary] = useState<string>('');
  const [trendingProducts, setTrendingProducts] = useState<TrendingProduct[]>([]);
  const [supplierMetrics, setSupplierMetrics] = useState<SupplierMetrics[]>([]);
  const [warehouseDetails, setWarehouseDetails] = useState<WarehouseDetail[]>([]);
  const [criticalCount, setCriticalCount] = useState<number>(propCriticalCount || 0);
  const [warningCount, setWarningCount] = useState<number>(propWarningCount || 0);
  const [isLoading, setIsLoading] = useState(true);
  const [alertsLoading, setAlertsLoading] = useState(false);

  const handleAskAI = () => {
    floatingGenieRef?.current?.openWithMessage("Hello PharmaStock AI, what's wrong with my inventory?");
  };

  useEffect(() => {
    // Only fetch critical counts if not provided via props
    if (propCriticalCount === undefined || propWarningCount === undefined) {
      fetchCriticalCounts();
    }
    // Fetch full homepage data
    fetchHomepageData();
  }, []);

  // Update state when props change
  useEffect(() => {
    if (propCriticalCount !== undefined) {
      setCriticalCount(propCriticalCount);
    }
    if (propWarningCount !== undefined) {
      setWarningCount(propWarningCount);
    }
  }, [propCriticalCount, propWarningCount]);

  const fetchCriticalCounts = async (showLoading = false) => {
    if (showLoading) setAlertsLoading(true);
    try {
      const response = await fetch('/api/homepage/critical-counts', {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
        },
        credentials: 'same-origin',
      });

      if (response.ok) {
        const data = await response.json();
        setCriticalCount(data.criticalCount || 0);
        setWarningCount(data.warningCount || 0);
      }
    } catch (error) {
      console.error('Error fetching critical counts:', error);
    } finally {
      if (showLoading) setAlertsLoading(false);
    }
  };

  const fetchHomepageData = async () => {
    setIsLoading(true);
    try {
      // Fetch data from API directly
      const response = await fetch('/api/homepage/data', {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
        },
        credentials: 'same-origin',
      });

      if (response.ok) {
        const data = await response.json();

        if (data) {
          setDailySummary(data.dailySummary);
          setTrendingProducts(data.trendingProducts);
          setSupplierMetrics(data.supplierMetrics);
          // Also update critical counts from full data (in case they differ)
          setCriticalCount(data.criticalCount || 0);
          setWarningCount(data.warningCount || 0);

          // Update warehouse details with API data
          if (data.warehouseDetails) {
            setWarehouseDetails(data.warehouseDetails);
          }
          return; // Exit early if successful
        }
      }
    } catch (error) {
      console.error('Error fetching homepage data:', error);
    }

    // Fallback to default data
    const today = new Date();
    const summary = `Good morning Elena! 👋 Today is ${format(today, 'EEEE, MMMM d, yyyy')}.
      Yesterday, we processed 127 transactions with a total value of €48,350.
      3 urgent reorders were triggered for critical components.
      The Hamburg cold chain facility reported a 15% increase in demand for biologic drug substances.
      Today's focus: Review 5 pending large orders and optimize Lyon GMP site inventory levels.`;

    setDailySummary(summary);

    // Default trending products
    const trending: TrendingProduct[] = [
      { sku: 'API0000001', name: 'Paclitaxel API', trend: 32, sales: 245 },
      { sku: 'BIO0000001', name: 'Insulin Bulk Drug Substance', trend: 28, sales: 189 },
      { sku: 'EXC0000001', name: 'Microcrystalline Cellulose PH102', trend: 24, sales: 156 },
      { sku: 'CTL0000001', name: 'Morphine Sulfate API', trend: -12, sales: 98 },
      { sku: 'PKG0000001', name: 'Vials 10mL Type I Glass', trend: 18, sales: 134 }
    ];
    setTrendingProducts(trending);

    // Default supplier metrics
    const supplierData: SupplierMetrics[] = [
      { supplier: 'Lonza Group', avgDays: 5.0, onTime: 99 },
      { supplier: 'Siegfried Holding AG', avgDays: 3.0, onTime: 97 },
      { supplier: 'Fareva SA', avgDays: 4.0, onTime: 95 },
      { supplier: 'Cambrex Corp', avgDays: 6.0, onTime: 92 },
      { supplier: 'PolyPeptide Group', avgDays: 5.5, onTime: 90 },
      { supplier: 'Recipharm AB', avgDays: 4.5, onTime: 93 }
    ];
    setSupplierMetrics(supplierData);

    setIsLoading(false);
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'operational': return 'text-green-600 bg-green-50 border-green-200';
      case 'maintenance': return 'text-yellow-600 bg-yellow-50 border-yellow-200';
      case 'critical': return 'text-red-600 bg-red-50 border-red-200';
      default: return 'text-gray-600 bg-gray-50 border-gray-200';
    }
  };

  // Warehouse data with coordinates for map
  const warehouses = [
    {
      id: 'lyon',
      name: 'Lyon GMP Manufacturing Site',
      location: 'Lyon, France',
      coordinates: [45.7640, 4.8357] as [number, number],
      capacity: 85000,
      currentStock: 68500,
      status: 'operational',
      manager: 'Marie Dubois',
      lastActivity: '2 hours ago',
      efficiency: 92
    },
    {
      id: 'hamburg',
      name: 'Hamburg Cold Chain Distribution Center',
      location: 'Hamburg, Germany',
      coordinates: [53.5511, 9.9937] as [number, number],
      capacity: 92000,
      currentStock: 78200,
      status: 'operational',
      manager: 'Klaus Schmidt',
      lastActivity: '45 minutes ago',
      efficiency: 88
    },
    {
      id: 'milan',
      name: 'Milan API & QC Hub',
      location: 'Milan, Italy',
      coordinates: [45.4642, 9.1900] as [number, number],
      capacity: 75000,
      currentStock: 62300,
      status: 'maintenance',
      manager: 'Giuseppe Rossi',
      lastActivity: '1 day ago',
      efficiency: 76
    }
  ];


  return (
    <div className="space-y-4">
      {/* First: KPI Cards - Elena's Metrics */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <Card className={`${
          kpis.onTimeProductionChange > 0 ? 'border-green-200 bg-green-50' :
          kpis.onTimeProductionChange < 0 ? 'border-red-200 bg-red-50' :
          'border-gray-200 bg-gray-50'
        }`}>
          <CardHeader className="pb-3">
            <div className="flex items-center justify-between">
              <CardTitle className={`text-sm font-medium ${
                kpis.onTimeProductionChange > 0 ? 'text-green-900' :
                kpis.onTimeProductionChange < 0 ? 'text-red-900' :
                'text-gray-900'
              }`}>
                On-Time Production Rate
              </CardTitle>
              <div className="flex items-center gap-1">
                <button
                  onClick={refreshOTPR}
                  disabled={otprLoading}
                  className="p-1 hover:bg-white/50 rounded transition-colors"
                  title="Refresh OTPR"
                >
                  {otprLoading ? (
                    <Loader2 className="w-4 h-4 animate-spin text-gray-600" />
                  ) : (
                    <RefreshCw className="w-4 h-4 text-gray-600 hover:text-gray-800" />
                  )}
                </button>
                <button
                  onClick={() => setActiveTab('analytics')}
                  className="p-1 hover:bg-white/50 rounded transition-colors"
                  title="View Analytics"
                >
                  <BarChart3 className="w-4 h-4 text-gray-600 hover:text-gray-800" />
                </button>
              </div>
            </div>
          </CardHeader>
          <CardContent>
            <div className="flex items-center justify-between">
              <div>
                <span className={`text-2xl font-bold ${
                  kpis.onTimeProductionChange > 0 ? 'text-green-700' :
                  kpis.onTimeProductionChange < 0 ? 'text-red-700' :
                  'text-gray-700'
                }`}>
                  {kpisLoading ? (
                    <span className="inline-flex items-center">
                      <Loader2 className="w-5 h-5 animate-spin mr-2" />
                      Updating...
                    </span>
                  ) : (
                    `${kpis.onTimeProductionRate.toFixed(1)}%`
                  )}
                </span>
                <div className="flex items-center gap-2 mt-1">
                  <span className={`text-sm font-medium flex items-center ${
                    kpis.onTimeProductionChange > 0 ? 'text-green-600' :
                    kpis.onTimeProductionChange < 0 ? 'text-red-600' :
                    'text-gray-600'
                  }`}>
                    <span className="mr-1">{kpis.onTimeProductionTrend}</span>
                    {kpis.onTimeProductionChange >= 0 ? '+' : ''}{kpis.onTimeProductionChange.toFixed(1)}%
                  </span>
                  <span className="text-xs text-gray-500">vs prev 30 days</span>
                </div>
              </div>
              {kpis.onTimeProductionChange > 0 ? (
                <TrendingUp className="w-5 h-5 text-green-600" />
              ) : kpis.onTimeProductionChange < 0 ? (
                <TrendingDown className="w-5 h-5 text-red-600" />
              ) : (
                <ArrowRight className="w-5 h-5 text-gray-600" />
              )}
            </div>
            <Progress
              value={kpis.onTimeProductionRate}
              className={`mt-2 ${
                kpis.onTimeProductionChange > 0 ? '[&>div]:bg-green-600' :
                kpis.onTimeProductionChange < 0 ? '[&>div]:bg-red-600' :
                '[&>div]:bg-gray-600'
              }`}
            />
            <p className="text-xs text-gray-600 mt-2">
              Previous 30 days: {kpis.onTimeProductionRatePrev.toFixed(1)}%
            </p>
          </CardContent>
        </Card>

        <Card className="border-blue-200 bg-blue-50">
          <CardHeader className="pb-3">
            <div className="flex items-center justify-between">
              <CardTitle className="text-sm font-medium text-blue-900">
                Inventory Turnover
              </CardTitle>
              <div className="flex items-center gap-1">
                <button
                  onClick={refreshInventoryTurnover}
                  disabled={turnoverLoading}
                  className="p-1 hover:bg-white/50 rounded transition-colors"
                  title="Refresh Inventory Turnover"
                >
                  {turnoverLoading ? (
                    <Loader2 className="w-4 h-4 animate-spin text-gray-600" />
                  ) : (
                    <RefreshCw className="w-4 h-4 text-gray-600 hover:text-gray-800" />
                  )}
                </button>
                <button
                  onClick={() => setActiveTab('analytics')}
                  className="p-1 hover:bg-white/50 rounded transition-colors"
                  title="View Analytics"
                >
                  <BarChart3 className="w-4 h-4 text-gray-600 hover:text-gray-800" />
                </button>
              </div>
            </div>
          </CardHeader>
          <CardContent>
            <div className="flex items-center justify-between">
              <span className="text-2xl font-bold text-blue-700">
                {kpis.inventoryTurnoverRatio}x
              </span>
              <TrendingUp className="w-5 h-5 text-blue-600" />
            </div>
            <p className="text-xs text-blue-600 mt-1">Target: 32x to 40x</p>
          </CardContent>
        </Card>

        <Card className="border-orange-200 bg-orange-50">
          <CardHeader className="pb-3">
            <div className="flex items-center justify-between">
              <CardTitle className="text-sm font-medium text-orange-900">
                Expedited Costs (MTD)
              </CardTitle>
              <div className="flex items-center gap-1">
                <button
                  onClick={refreshExpeditedCosts}
                  disabled={expeditedLoading}
                  className="p-1 hover:bg-white/50 rounded transition-colors"
                  title="Refresh Expedited Costs"
                >
                  {expeditedLoading ? (
                    <Loader2 className="w-4 h-4 animate-spin text-gray-600" />
                  ) : (
                    <RefreshCw className="w-4 h-4 text-gray-600 hover:text-gray-800" />
                  )}
                </button>
                <button
                  onClick={() => setActiveTab('analytics')}
                  className="p-1 hover:bg-white/50 rounded transition-colors"
                  title="View Analytics"
                >
                  <BarChart3 className="w-4 h-4 text-gray-600 hover:text-gray-800" />
                </button>
              </div>
            </div>
          </CardHeader>
          <CardContent>
            <div className="flex items-center justify-between">
              <span className="text-2xl font-bold text-orange-700">
                €{kpis.expeditedShipmentsCost.toLocaleString()}
              </span>
              <Truck className="w-5 h-5 text-orange-600" />
            </div>
            <p className="text-xs text-orange-600 mt-1">+23% vs last month</p>
          </CardContent>
        </Card>

        <Card className="border-purple-200 bg-purple-50">
          <CardHeader className="pb-3">
            <div className="flex items-center justify-between">
              <CardTitle className="text-sm font-medium text-purple-900">
                Days of Stock
              </CardTitle>
              <div className="flex items-center gap-1">
                <button
                  onClick={refreshDaysOfStock}
                  disabled={daysOfStockLoading}
                  className="p-1 hover:bg-white/50 rounded transition-colors"
                  title="Refresh Days of Stock"
                >
                  {daysOfStockLoading ? (
                    <Loader2 className="w-4 h-4 animate-spin text-gray-600" />
                  ) : (
                    <RefreshCw className="w-4 h-4 text-gray-600 hover:text-gray-800" />
                  )}
                </button>
                <button
                  onClick={() => setActiveTab('analytics')}
                  className="p-1 hover:bg-white/50 rounded transition-colors"
                  title="View Analytics"
                >
                  <BarChart3 className="w-4 h-4 text-gray-600 hover:text-gray-800" />
                </button>
              </div>
            </div>
          </CardHeader>
          <CardContent>
            <div className="flex items-center justify-between">
              <span className="text-2xl font-bold text-purple-700">
                {kpis.daysOfStockOnHand} days
              </span>
              <Clock className="w-5 h-5 text-purple-600" />
            </div>
            <p className="text-xs text-purple-600 mt-1">Target: 15-20 days</p>
          </CardContent>
        </Card>
      </div>

      {/* Second: Inventory Status Section */}
      {(criticalCount > 0 || warningCount > 0) ? (
        <Alert className="bg-red-50 border-red-200">
          <AlertTriangle className="h-4 w-4 text-red-600" />
          <AlertTitle className="text-red-900 flex items-center justify-between">
            <span>Action Required</span>
            <button
              onClick={() => fetchCriticalCounts(true)}
              disabled={alertsLoading}
              className="p-1 hover:bg-red-100 rounded transition-colors"
              title="Refresh alerts"
            >
              {alertsLoading ? (
                <Loader2 className="w-4 h-4 animate-spin text-red-600" />
              ) : (
                <RefreshCw className="w-4 h-4 text-red-600 hover:text-red-800" />
              )}
            </button>
          </AlertTitle>
          <AlertDescription className="text-red-700 flex items-center justify-between">
            <span>
              {criticalCount > 0 && <span>{criticalCount} components need immediate attention</span>}
              {criticalCount > 0 && warningCount > 0 && <span> and </span>}
              {warningCount > 0 && <span>{warningCount} require monitoring</span>}
            </span>
            <div className="flex items-center gap-2 ml-4">
              <button
                onClick={handleAskAI}
                className="text-white bg-gradient-to-r from-blue-600 to-indigo-600 hover:from-blue-700 hover:to-indigo-700 font-semibold px-4 py-2 rounded-lg flex items-center gap-2 transition-all duration-300 shadow-md hover:shadow-lg transform hover:scale-105"
              >
                <Bot className="h-4 w-4" />
                <span>Ask your AI</span>
              </button>
              <button
                onClick={() => setActiveTab('forecast')}
                className="text-red-800 font-semibold hover:text-red-900 flex items-center gap-1 transition-colors"
              >
                View Details
                <ArrowRight className="h-4 w-4" />
              </button>
            </div>
          </AlertDescription>
        </Alert>
      ) : (
        <Alert className="bg-green-50 border-green-200">
          <CheckCircle className="h-4 w-4 text-green-600" />
          <AlertTitle className="text-green-900 flex items-center justify-between">
            <span>Inventory Optimized</span>
            <button
              onClick={() => fetchCriticalCounts(true)}
              disabled={alertsLoading}
              className="p-1 hover:bg-green-100 rounded transition-colors"
              title="Refresh alerts"
            >
              {alertsLoading ? (
                <Loader2 className="w-4 h-4 animate-spin text-green-600" />
              ) : (
                <RefreshCw className="w-4 h-4 text-green-600 hover:text-green-800" />
              )}
            </button>
          </AlertTitle>
          <AlertDescription className="text-green-700 flex items-center justify-between">
            <span>
              All inventory levels are healthy. No components require immediate attention or monitoring.
            </span>
            <button
              onClick={() => setActiveTab('forecast')}
              className="text-green-800 font-semibold hover:text-green-900 flex items-center gap-1 transition-colors ml-4"
            >
              View Forecast
              <ArrowRight className="h-4 w-4" />
            </button>
          </AlertDescription>
        </Alert>
      )}

      {/* Third: Yesterday's Highlights and Today's Insights - Full Width */}
      <Card className="relative overflow-hidden border-0 shadow-lg">
        <div className="absolute top-0 left-0 w-full h-1 bg-gradient-to-r from-blue-500 via-indigo-500 to-purple-500" />
        <CardHeader className="pb-3">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <div className="p-2 bg-blue-100 rounded-lg">
                <Sparkles className="h-5 w-5 text-blue-600" />
              </div>
              <div>
                <CardTitle className="text-lg font-semibold">Insights & Analytics</CardTitle>
                <p className="text-xs text-gray-500 mt-1">Good morning, Elena</p>
              </div>
            </div>
            <Badge variant="outline" className="text-xs bg-white">
              <Clock className="h-3 w-3 mr-1" />
              Updated {format(new Date(), 'HH:mm')}
            </Badge>
          </div>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {/* Yesterday's Summary */}
            <div className="p-4 bg-blue-50 rounded-lg border border-blue-100">
              <div className="space-y-2">
                <div className="flex items-center gap-2">
                  <Activity className="h-5 w-5 text-blue-600 flex-shrink-0" />
                  <span className="font-semibold text-blue-900 text-sm">Yesterday's Highlights</span>
                </div>
                <ul className="text-gray-700 space-y-1 ml-7 text-sm">
                  <li>• 127 transactions processed with total value of €48,350</li>
                  <li>• On-time production rate: 92% (exceeded target)</li>
                  <li>• Hamburg cold chain facility showed +15% demand for biologic drug substances</li>
                  <li>• Zero critical stock-outs across all warehouses</li>
                </ul>
              </div>
            </div>

            {/* Today's Focus */}
            <div className="p-4 bg-green-50 rounded-lg border border-green-100">
              <div className="space-y-2">
                <div className="flex items-center gap-2">
                  <ArrowRight className="h-5 w-5 text-green-600 flex-shrink-0" />
                  <span className="font-semibold text-green-900 text-sm">Today's Priorities</span>
                </div>
                <ul className="text-gray-700 space-y-1 ml-7 text-sm">
                  <li>• Review 5 pending orders worth €125,000</li>
                  <li>• Optimize Lyon warehouse inventory levels</li>
                  <li>• TechParts supplier meeting at 2:00 PM</li>
                  <li>• Monitor critical components for reorder needs</li>
                </ul>
              </div>
            </div>
          </div>

          <div className="flex justify-end mt-4 pt-4 border-t">
            <button
              onClick={() => setActiveTab('analytics')}
              className="text-sm text-blue-600 hover:text-blue-800 font-medium flex items-center gap-1"
            >
              View Detailed Analytics
              <ArrowRight className="h-4 w-4" />
            </button>
          </div>
        </CardContent>
      </Card>

      {/* Fourth: Interactive Warehouse Map */}
      <div>
        <h2 className="text-lg font-semibold mb-4 flex items-center gap-2">
          <MapPin className="h-5 w-5" />
          Warehouse Network Map
        </h2>
        <Card>
          <CardContent className="p-0">
            <Suspense fallback={
              <div className="flex items-center justify-center h-[500px]">
                <div className="flex items-center gap-2">
                  <Loader2 className="h-6 w-6 animate-spin" />
                  <span>Loading warehouse map...</span>
                </div>
              </div>
            }>
              <LeafletWarehouseMap warehouses={warehouses} supplyRoutes={[]} />
            </Suspense>
          </CardContent>
        </Card>
      </div>
    </div>
  );
};

export default Homepage;