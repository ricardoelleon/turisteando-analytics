const express = require('express');
const cors = require('cors');
const { BetaAnalyticsDataClient } = require('@google-analytics/data');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());
app.use(express.static('public'));

const analyticsDataClient = new BetaAnalyticsDataClient({
  credentials: {
    client_email: process.env.SERVICE_ACCOUNT_EMAIL,
    private_key: process.env.SERVICE_ACCOUNT_PRIVATE_KEY.replace(/\\n/g, '\n'),
  },
});

const PROPERTY_ID = process.env.PROPERTY_ID || '487082948';

// ========================================
// HELPER FUNCTIONS
// ========================================

async function runReport(dimensions, metrics, dateRange, orderBy = null, limit = 10) {
  const request = {
    property: `properties/${PROPERTY_ID}`,
    dateRanges: [{ startDate: dateRange.startDate, endDate: dateRange.endDate }],
    dimensions: dimensions.map(d => ({ name: d })),
    metrics: metrics.map(m => ({ name: m })),
    limit: limit,
  };
  
  if (orderBy) {
    request.orderBys = [{ metric: { metricName: orderBy }, desc: true }];
  }
  
  return await analyticsDataClient.runReport(request);
}

async function runReportWithFilter(dimensions, metrics, dateRange, filter, orderBy = null, limit = 10) {
  const request = {
    property: `properties/${PROPERTY_ID}`,
    dateRanges: [{ startDate: dateRange.startDate, endDate: dateRange.endDate }],
    dimensions: dimensions.map(d => ({ name: d })),
    metrics: metrics.map(m => ({ name: m })),
    dimensionFilter: filter,
    limit: limit,
  };
  
  if (orderBy) {
    request.orderBys = [{ metric: { metricName: orderBy }, desc: true }];
  }
  
  return await analyticsDataClient.runReport(request);
}

// ========================================
// BASIC ENDPOINTS
// ========================================

app.get('/api/health', (req, res) => {
  res.json({ status: 'OK', timestamp: new Date().toISOString() });
});

// Overview - KPIs generales
app.get('/api/analytics/overview', async (req, res) => {
  try {
    const { startDate = '7daysAgo', endDate = 'today' } = req.query;
    
    const [activeUsersReport, newUsersReport, sessionsReport, pageViewsReport, durationReport, bounceReport, engagementReport] = await Promise.all([
      runReport([], ['activeUsers'], { startDate, endDate }),
      runReport([], ['newUsers'], { startDate, endDate }),
      runReport([], ['sessions'], { startDate, endDate }),
      runReport([], ['screenPageViews'], { startDate, endDate }),
      runReport([], ['averageSessionDuration'], { startDate, endDate }),
      runReport([], ['bounceRate'], { startDate, endDate }),
      runReport([], ['engagementRate'], { startDate, endDate }),
    ]);
    
    const getValue = (report, metricIndex = 0) => {
      if (report[0].rows && report[0].rows[0]) {
        return parseFloat(report[0].rows[0].metricValues[metricIndex].value) || 0;
      }
      return 0;
    };
    
    res.json({
      success: true,
      data: {
        activeUsers: getValue(activeUsersReport),
        newUsers: getValue(newUsersReport),
        sessions: getValue(sessionsReport),
        pageViews: getValue(pageViewsReport),
        avgSessionDuration: getValue(durationReport),
        bounceRate: getValue(bounceReport) * 100,
        engagementRate: getValue(engagementReport),
      }
    });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// Users by day
app.get('/api/analytics/users-by-day', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    
    const report = await runReport(['date'], ['activeUsers', 'newUsers', 'sessions'], { startDate, endDate }, null, 30);
    
    const data = report[0].rows?.map(row => ({
      date: row.dimensionValues[0].value,
      users: parseInt(row.metricValues[0].value) || 0,
      newUsers: parseInt(row.metricValues[1].value) || 0,
      sessions: parseInt(row.metricValues[2].value) || 0,
    })) || [];
    
    res.json({ success: true, data });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// By platform (Android/iOS)
app.get('/api/analytics/by-platform', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    
    const report = await runReport(['platform'], ['activeUsers', 'newUsers', 'sessions', 'screenPageViews'], { startDate, endDate }, 'activeUsers');
    
    const platforms = report[0].rows?.map(row => ({
      platform: row.dimensionValues[0].value,
      users: parseInt(row.metricValues[0].value) || 0,
      newUsers: parseInt(row.metricValues[1].value) || 0,
      sessions: parseInt(row.metricValues[2].value) || 0,
      screens: parseInt(row.metricValues[3].value) || 0,
    })) || [];
    
    res.json({ success: true, data: platforms });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// By country
app.get('/api/analytics/by-country', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    
    const report = await runReport(['country'], ['activeUsers', 'newUsers'], { startDate, endDate }, 'activeUsers', 15);
    
    const countries = report[0].rows?.map(row => ({
      country: row.dimensionValues[0].value,
      users: parseInt(row.metricValues[0].value) || 0,
      newUsers: parseInt(row.metricValues[1].value) || 0,
    })) || [];
    
    res.json({ success: true, data: countries });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// ========================================
// CUSTOM EVENTS - TURISTEANDO APP
// ========================================

// Top events (general)
app.get('/api/analytics/top-events', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    
    const report = await runReport(['eventName'], ['eventCount', 'activeUsers'], { startDate, endDate }, 'eventCount', 20);
    
    const events = report[0].rows?.map(row => ({
      event: row.dimensionValues[0].value,
      count: parseInt(row.metricValues[0].value) || 0,
      users: parseInt(row.metricValues[1].value) || 0,
    })) || [];
    
    res.json({ success: true, data: events });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// PUEBLOS - Vistas por pueblo
app.get('/api/analytics/pueblos', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    
    // Event: pueblo_view with dimension pueblo_nombre
    const report = await runReportWithFilter(
      ['customEvent:pueblo_nombre'],
      ['eventCount', 'activeUsers'],
      { startDate, endDate },
      {
        filter: {
          fieldName: 'eventName',
          stringFilter: { value: 'pueblo_view' }
        }
      },
      'eventCount', 20
    );
    
    const pueblos = report[0].rows?.map(row => ({
      pueblo: row.dimensionValues[0].value,
      views: parseInt(row.metricValues[0].value) || 0,
      users: parseInt(row.metricValues[1].value) || 0,
    })) || [];
    
    res.json({ success: true, data: pueblos });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// CATEGORIAS - Vistas por categoría
app.get('/api/analytics/categorias', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    
    const report = await runReportWithFilter(
      ['customEvent:category_name', 'customEvent:pueblo_id'],
      ['eventCount', 'activeUsers'],
      { startDate, endDate },
      {
        filter: {
          fieldName: 'eventName',
          stringFilter: { value: 'category_view' }
        }
      },
      'eventCount', 30
    );
    
    const categorias = report[0].rows?.map(row => ({
      categoria: row.dimensionValues[0].value,
      pueblo: row.dimensionValues[1].value,
      views: parseInt(row.metricValues[0].value) || 0,
      users: parseInt(row.metricValues[1].value) || 0,
    })) || [];
    
    res.json({ success: true, data: categorias });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// ENTIDADES - Más visitadas
app.get('/api/analytics/entidades', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    
    const report = await runReportWithFilter(
      ['customEvent:entity_name', 'customEvent:category_name', 'customEvent:pueblo_id'],
      ['eventCount', 'activeUsers'],
      { startDate, endDate },
      {
        filter: {
          fieldName: 'eventName',
          stringFilter: { value: 'entity_clicked' }
        }
      },
      'eventCount', 30
    );
    
    const entidades = report[0].rows?.map(row => ({
      entidad: row.dimensionValues[0].value,
      categoria: row.dimensionValues[1].value,
      pueblo: row.dimensionValues[2].value,
      clicks: parseInt(row.metricValues[0].value) || 0,
      users: parseInt(row.metricValues[1].value) || 0,
    })) || [];
    
    res.json({ success: true, data: entidades });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// ACCIONES - Llamadas, WhatsApp, Mapas, etc.
app.get('/api/analytics/acciones', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    
    const report = await runReportWithFilter(
      ['customEvent:action', 'customEvent:entity_name', 'customEvent:category_id'],
      ['eventCount'],
      { startDate, endDate },
      {
        filter: {
          fieldName: 'eventName',
          stringFilter: { value: 'entity_action' }
        }
      },
      'eventCount', 50
    );
    
    const acciones = report[0].rows?.map(row => ({
      action: row.dimensionValues[0].value,
      entidad: row.dimensionValues[1].value,
      categoria: row.dimensionValues[2].value,
      count: parseInt(row.metricValues[0].value) || 0,
    })) || [];
    
    res.json({ success: true, data: acciones });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// Resumen de acciones por tipo
app.get('/api/analytics/acciones-resumen', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    
    const report = await runReportWithFilter(
      ['customEvent:action'],
      ['eventCount'],
      { startDate, endDate },
      {
        filter: {
          fieldName: 'eventName',
          stringFilter: { value: 'entity_action' }
        }
      },
      'eventCount', 20
    );
    
    const resumen = report[0].rows?.map(row => ({
      action: row.dimensionValues[0].value,
      count: parseInt(row.metricValues[0].value) || 0,
    })) || [];
    
    res.json({ success: true, data: resumen });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// BÚSQUEDAS
app.get('/api/analytics/busquedas', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    
    const report = await runReportWithFilter(
      ['customEvent:search_term'],
      ['eventCount', 'activeUsers'],
      { startDate, endDate },
      {
        filter: {
          fieldName: 'eventName',
          stringFilter: { value: 'search' }
        }
      },
      'eventCount', 30
    );
    
    const busquedas = report[0].rows?.map(row => ({
      query: row.dimensionValues[0].value,
      count: parseInt(row.metricValues[0].value) || 0,
      users: parseInt(row.metricValues[1].value) || 0,
    })) || [];
    
    res.json({ success: true, data: busquedas });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// ERRORES
app.get('/api/analytics/errores', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    
    const report = await runReportWithFilter(
      ['customEvent:error_type', 'customEvent:error_message'],
      ['eventCount'],
      { startDate, endDate },
      {
        filter: {
          fieldName: 'eventName',
          stringFilter: { value: 'app_error' }
        }
      },
      'eventCount', 20
    );
    
    const errores = report[0].rows?.map(row => ({
      tipo: row.dimensionValues[0].value,
      mensaje: row.dimensionValues[1].value,
      count: parseInt(row.metricValues[0].value) || 0,
    })) || [];
    
    res.json({ success: true, data: errores });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// SOS - Emergencias
app.get('/api/analytics/sos', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    
    const report = await runReportWithFilter(
      ['customEvent:sos_type', 'customEvent:action'],
      ['eventCount'],
      { startDate, endDate },
      {
        filter: {
          fieldName: 'eventName',
          stringFilter: { value: 'sos_action' }
        }
      },
      'eventCount', 20
    );
    
    const sos = report[0].rows?.map(row => ({
      tipo: row.dimensionValues[0].value,
      action: row.dimensionValues[1].value,
      count: parseInt(row.metricValues[0].value) || 0,
    })) || [];
    
    res.json({ success: true, data: sos });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// COMPARTIR
app.get('/api/analytics/compartir', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    
    const report = await runReportWithFilter(
      ['customEvent:share_method', 'customEvent:entity_name'],
      ['eventCount'],
      { startDate, endDate },
      {
        filter: {
          fieldName: 'eventName',
          stringFilter: { value: 'share' }
        }
      },
      'eventCount', 20
    );
    
    const compartir = report[0].rows?.map(row => ({
      metodo: row.dimensionValues[0].value,
      entidad: row.dimensionValues[1].value,
      count: parseInt(row.metricValues[0].value) || 0,
    })) || [];
    
    res.json({ success: true, data: compartir });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// SCREEN VIEWS - Pantallas más vistas
app.get('/api/analytics/top-screens', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    
    const report = await runReport(['screenName'], ['screenPageViews', 'activeUsers'], { startDate, endDate }, 'screenPageViews', 20);
    
    const screens = report[0].rows?.map(row => ({
      screen: row.dimensionValues[0].value,
      views: parseInt(row.metricValues[0].value) || 0,
      users: parseInt(row.metricValues[1].value) || 0,
    })) || [];
    
    res.json({ success: true, data: screens });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// ACQUISITION - Fuentes de tráfico
app.get('/api/analytics/acquisition', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    
    const report = await runReport(['sessionDefaultChannelGroup'], ['sessions', 'activeUsers'], { startDate, endDate }, 'sessions', 10);
    
    const sources = report[0].rows?.map(row => ({
      source: row.dimensionValues[0].value,
      sessions: parseInt(row.metricValues[0].value) || 0,
      users: parseInt(row.metricValues[1].value) || 0,
    })) || [];
    
    res.json({ success: true, data: sources });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// DASHBOARD COMPLETO - Todo en un endpoint
app.get('/api/analytics/dashboard', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    const dateRange = { startDate, endDate };
    
    // Ejecutar todos los reports en paralelo
    const [
      overviewData,
      pueblosData,
      categoriasData,
      entidadesData,
      accionesData,
      busquedasData,
      erroresData,
      sosData,
      platformsData,
      countriesData
    ] = await Promise.all([
      // Overview
      runReport([], ['activeUsers', 'newUsers', 'sessions', 'screenPageViews', 'averageSessionDuration', 'bounceRate', 'engagementRate'], dateRange),
      // Pueblos
      runReportWithFilter(['customEvent:pueblo_nombre'], ['eventCount', 'activeUsers'], dateRange, 
        { filter: { fieldName: 'eventName', stringFilter: { value: 'pueblo_view' } } }, 'eventCount', 15).catch(() => null),
      // Categorias
      runReportWithFilter(['customEvent:category_name'], ['eventCount', 'activeUsers'], dateRange,
        { filter: { fieldName: 'eventName', stringFilter: { value: 'category_view' } } }, 'eventCount', 15).catch(() => null),
      // Entidades
      runReportWithFilter(['customEvent:entity_name'], ['eventCount', 'activeUsers'], dateRange,
        { filter: { fieldName: 'eventName', stringFilter: { value: 'entity_clicked' } } }, 'eventCount', 20).catch(() => null),
      // Acciones
      runReportWithFilter(['customEvent:action'], ['eventCount'], dateRange,
        { filter: { fieldName: 'eventName', stringFilter: { value: 'entity_action' } } }, 'eventCount', 15).catch(() => null),
      // Busquedas
      runReportWithFilter(['customEvent:search_term'], ['eventCount'], dateRange,
        { filter: { fieldName: 'eventName', stringFilter: { value: 'search' } } }, 'eventCount', 15).catch(() => null),
      // Errores
      runReportWithFilter(['customEvent:error_type'], ['eventCount'], dateRange,
        { filter: { fieldName: 'eventName', stringFilter: { value: 'app_error' } } }, 'eventCount', 10).catch(() => null),
      // SOS
      runReportWithFilter(['customEvent:sos_type'], ['eventCount'], dateRange,
        { filter: { fieldName: 'eventName', stringFilter: { value: 'sos_action' } } }, 'eventCount', 10).catch(() => null),
      // Platforms
      runReport(['platform'], ['activeUsers', 'sessions'], dateRange, 'activeUsers', 5).catch(() => null),
      // Countries
      runReport(['country'], ['activeUsers'], dateRange, 'activeUsers', 10).catch(() => null)
    ]);
    
    // Parse overview
    const overview = {
      activeUsers: overviewData[0].rows?.[0]?.metricValues?.[0]?.value || 0,
      newUsers: overviewData[0].rows?.[0]?.metricValues?.[1]?.value || 0,
      sessions: overviewData[0].rows?.[0]?.metricValues?.[2]?.value || 0,
      pageViews: overviewData[0].rows?.[0]?.metricValues?.[3]?.value || 0,
      avgSessionDuration: parseFloat(overviewData[0].rows?.[0]?.metricValues?.[4]?.value) || 0,
      bounceRate: parseFloat(overviewData[0].rows?.[0]?.metricValues?.[5]?.value) * 100 || 0,
      engagementRate: parseFloat(overviewData[0].rows?.[0]?.metricValues?.[6]?.value) || 0,
    };
    
    res.json({
      success: true,
      data: {
        overview,
        pueblos: pueblosData?.[0]?.rows?.map(r => ({
          pueblo: r.dimensionValues[0].value,
          views: parseInt(r.metricValues[0].value) || 0,
          users: parseInt(r.metricValues[1].value) || 0
        })) || [],
        categorias: categoriasData?.[0]?.rows?.map(r => ({
          categoria: r.dimensionValues[0].value,
          views: parseInt(r.metricValues[0].value) || 0,
          users: parseInt(r.metricValues[1].value) || 0
        })) || [],
        entidades: entidadesData?.[0]?.rows?.map(r => ({
          entidad: r.dimensionValues[0].value,
          clicks: parseInt(r.metricValues[0].value) || 0,
          users: parseInt(r.metricValues[1].value) || 0
        })) || [],
        acciones: accionesData?.[0]?.rows?.map(r => ({
          action: r.dimensionValues[0].value,
          count: parseInt(r.metricValues[0].value) || 0
        })) || [],
        busquedas: busquedasData?.[0]?.rows?.map(r => ({
          query: r.dimensionValues[0].value,
          count: parseInt(r.metricValues[0].value) || 0
        })) || [],
        errores: erroresData?.[0]?.rows?.map(r => ({
          tipo: r.dimensionValues[0].value,
          count: parseInt(r.metricValues[0].value) || 0
        })) || [],
        sos: sosData?.[0]?.rows?.map(r => ({
          tipo: r.dimensionValues[0].value,
          count: parseInt(r.metricValues[0].value) || 0
        })) || [],
        platforms: platformsData?.[0]?.rows?.map(r => ({
          platform: r.dimensionValues[0].value,
          users: parseInt(r.metricValues[0].value) || 0,
          sessions: parseInt(r.metricValues[1].value) || 0
        })) || [],
        countries: countriesData?.[0]?.rows?.map(r => ({
          country: r.dimensionValues[0].value,
          users: parseInt(r.metricValues[0].value) || 0
        })) || []
      }
    });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

app.listen(PORT, () => {
  console.log(`🚀 Turisteando Analytics Server running on port ${PORT}`);
});
