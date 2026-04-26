const express = require('express');
const cors = require('cors');
const { BetaAnalyticsDataClient } = require('@google-analytics/data');
const { MongoClient } = require('mongodb');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());
app.use(express.static('public'));

// ========================================
// FIREBASE ANALYTICS (tu configuración existente)
// ========================================

const analyticsDataClient = new BetaAnalyticsDataClient({
  credentials: {
    client_email: process.env.SERVICE_ACCOUNT_EMAIL,
    private_key: process.env.SERVICE_ACCOUNT_PRIVATE_KEY.replace(/\\n/g, '\n'),
  },
});

const PROPERTY_ID = process.env.PROPERTY_ID || '487082948';

// ========================================
// MONGODB ATLAS (nueva conexión)
// ========================================

const MONGO_URI = process.env.MONGODB_URI;
let mongoClient = null;
let mongoDb = null;

async function connectMongoDB() {
  if (!MONGO_URI) {
    console.log('⚠️ MONGODB_URI no configurado - MongoDB deshabilitado');
    return false;
  }
  
  try {
    mongoClient = new MongoClient(MONGO_URI);
    await mongoClient.connect();
    mongoDb = mongoClient.db('turisteando_analytics');
    console.log('✅ MongoDB Atlas conectado');
    return true;
  } catch (error) {
    console.error('❌ Error conectando MongoDB:', error.message);
    return false;
  }
}

// Conectar al iniciar
connectMongoDB();

// ========================================
// HELPER FUNCTIONS - MEJORADOS Y DINÁMICOS
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

// Función helper para extraer valores de forma segura
function extractValue(row, index, defaultValue = '') {
  return row.dimensionValues?.[index]?.value || defaultValue;
}

function extractMetric(row, index, defaultValue = 0) {
  return parseInt(row.metricValues?.[index]?.value) || defaultValue;
}

// ========================================
// MIDDLEWARE PARA LOGGING
// ========================================

app.use((req, res, next) => {
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.path}`);
  next();
});

// ========================================
// BASIC ENDPOINTS
// ========================================

app.get('/api/health', (req, res) => {
  res.json({ 
    status: 'OK', 
    timestamp: new Date().toISOString(),
    propertyId: PROPERTY_ID,
    mongodb: mongoDb ? 'connected' : 'disconnected',
    version: '4.0.0-flexible' // Versión actualizada
  });
});

// Overview - KPIs generales
app.get('/api/analytics/overview', async (req, res) => {
  try {
    const { startDate = '7daysAgo', endDate = 'today' } = req.query;
    
    const [report] = await runReport(
      [], 
      ['activeUsers', 'newUsers', 'sessions', 'screenPageViews', 'averageSessionDuration', 'bounceRate', 'engagementRate'], 
      { startDate, endDate }
    );
    
    const row = report.rows?.[0];
    const getValue = (idx, mult = 1) => parseFloat(row?.metricValues?.[idx]?.value || 0) * mult;
    
    res.json({
      success: true,
      data: {
        activeUsers: getValue(0),
        newUsers: getValue(1),
        sessions: getValue(2),
        pageViews: getValue(3),
        avgSessionDuration: getValue(4),
        bounceRate: getValue(5, 100),
        engagementRate: getValue(6),
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
    
    const [report] = await runReport(['date'], ['activeUsers', 'newUsers', 'sessions'], { startDate, endDate }, null, 30);
    
    const data = report.rows?.map(row => ({
      date: extractValue(row, 0),
      users: extractMetric(row, 0),
      newUsers: extractMetric(row, 1),
      sessions: extractMetric(row, 2),
    })) || [];
    
    res.json({ success: true, data });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// By platform
app.get('/api/analytics/by-platform', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    
    const [report] = await runReport(['platform'], ['activeUsers', 'newUsers', 'sessions', 'screenPageViews'], { startDate, endDate }, 'activeUsers');
    
    const data = report.rows?.map(row => ({
      platform: extractValue(row, 0),
      users: extractMetric(row, 0),
      newUsers: extractMetric(row, 1),
      sessions: extractMetric(row, 2),
      screens: extractMetric(row, 3),
    })) || [];
    
    res.json({ success: true, data });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// By country
app.get('/api/analytics/by-country', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    
    const [report] = await runReport(['country'], ['activeUsers', 'newUsers'], { startDate, endDate }, 'activeUsers', 15);
    
    const data = report.rows?.map(row => ({
      country: extractValue(row, 0),
      users: extractMetric(row, 0),
      newUsers: extractMetric(row, 1),
    })) || [];
    
    res.json({ success: true, data });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// Top events (general)
app.get('/api/analytics/top-events', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    
    const [report] = await runReport(['eventName'], ['eventCount', 'activeUsers'], { startDate, endDate }, 'eventCount', 30);
    
    const data = report.rows?.map(row => ({
      event: extractValue(row, 0),
      count: extractMetric(row, 0),
      users: extractMetric(row, 1),
    })) || [];
    
    res.json({ success: true, data });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// Top screens
app.get('/api/analytics/top-screens', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    
    const [report] = await runReport(['screenName'], ['screenPageViews', 'activeUsers'], { startDate, endDate }, 'screenPageViews', 20);
    
    const data = report.rows?.map(row => ({
      screen: extractValue(row, 0),
      views: extractMetric(row, 0),
      users: extractMetric(row, 1),
    })) || [];
    
    res.json({ success: true, data });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// ========================================
// ENDPOINTS DINÁMICOS - TURISTEANDO APP
// ========================================

// EVENTOS DESCUBIERTOS AUTOMÁTICAMENTE
app.get('/api/analytics/eventos-descubiertos', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    
    const [eventsReport] = await runReport(['eventName'], ['eventCount'], { startDate, endDate }, 'eventCount', 100);
    
    const eventos = [];
    const customEventPrefixes = ['pueblo_', 'category_', 'entity_', 'abrir_', 'sos_', 'button_', 'filter', 'search'];
    
    for (const row of eventsReport.rows || []) {
      const eventName = extractValue(row, 0);
      const count = extractMetric(row, 0);
      
      const isCustom = customEventPrefixes.some(prefix => eventName.startsWith(prefix)) ||
                       eventName.includes('_action') ||
                       eventName.includes('_clicked') ||
                       eventName.includes('_view');
      
      if (isCustom && count > 0) {
        eventos.push({
          event: eventName,
          count: count
        });
      }
    }
    
    res.json({ success: true, data: eventos });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// PUEBLOS - Dinámico
app.get('/api/analytics/pueblos', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    
    let pueblos = [];
    
    try {
      const [report] = await runReportWithFilter(
        ['customEvent:pueblo_nombre'],
        ['eventCount', 'activeUsers'],
        { startDate, endDate },
        {
          filter: {
            fieldName: 'eventName',
            stringFilter: { value: 'pueblo_view' }
          }
        },
        'eventCount', 30
      );
      
      pueblos = report.rows?.map(row => ({
        pueblo: extractValue(row, 0),
        views: extractMetric(row, 0),
        users: extractMetric(row, 1),
      })) || [];
    } catch (e) {
      try {
        const [screenReport] = await runReport(['screenName'], ['screenPageViews', 'activeUsers'], { startDate, endDate }, 'screenPageViews', 50);
        
        pueblos = screenReport.rows
          ?.filter(row => {
            const screen = extractValue(row, 0).toLowerCase();
            return screen.includes('pueblo_') || screen.includes('Screen');
          })
          .map(row => ({
            pueblo: extractValue(row, 0).replace('Pueblo_', '').replace('Screen', ''),
            views: extractMetric(row, 0),
            users: extractMetric(row, 1),
          })) || [];
      } catch (e2) {
        pueblos = [];
      }
    }
    
    res.json({ success: true, data: pueblos });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// CATEGORÍAS - Dinámico con parámetros
app.get('/api/analytics/categorias', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    
    let categorias = [];
    
    try {
      const [report] = await runReportWithFilter(
        ['customEvent:category_name', 'customEvent:pueblo_id'],
        ['eventCount', 'activeUsers'],
        { startDate, endDate },
        {
          filter: {
            fieldName: 'eventName',
            stringFilter: { value: 'category_clicked' }
          }
        },
        'eventCount', 50
      );
      
      categorias = report.rows?.map(row => ({
        categoria: extractValue(row, 0),
        pueblo: extractValue(row, 1),
        clicks: extractMetric(row, 0),
        users: extractMetric(row, 1),
      })) || [];
    } catch (e) {
      const [eventsReport] = await runReport(['eventName'], ['eventCount', 'activeUsers'], { startDate, endDate }, 'eventCount', 50);
      
      const categoriasEvents = ['turismo_clicked', 'restaurante_clicked', 'comercio_clicked', 'hotel_clicked', 'category_clicked'];
      
      categorias = eventsReport.rows
        ?.filter(row => categoriasEvents.some(e => extractValue(row, 0).includes(e.replace('_clicked', ''))))
        .map(row => {
          let nombre = extractValue(row, 0).replace('_clicked', '');
          return {
            categoria: nombre.charAt(0).toUpperCase() + nombre.slice(1),
            clicks: extractMetric(row, 0),
            users: extractMetric(row, 1),
          };
        }) || [];
    }
    
    res.json({ success: true, data: categorias });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// VISTA DE CATEGORÍAS (category_view)
app.get('/api/analytics/categorias-vistas', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    
    let vistas = [];
    
    try {
      const [report] = await runReportWithFilter(
        ['customEvent:category_name', 'customEvent:pueblo_id'],
        ['eventCount', 'activeUsers'],
        { startDate, endDate },
        {
          filter: {
            fieldName: 'eventName',
            stringFilter: { value: 'category_view' }
          }
        },
        'eventCount', 50
      );
      
      vistas = report.rows?.map(row => ({
        categoria: extractValue(row, 0),
        pueblo: extractValue(row, 1),
        views: extractMetric(row, 0),
        users: extractMetric(row, 1),
      })) || [];
    } catch (e) {
      vistas = [];
    }
    
    res.json({ success: true, data: vistas });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// ENTIDADES (lugares)
app.get('/api/analytics/entidades', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today', puebloId } = req.query;
    
    let entidades = [];
    
    let filter = {
      filter: {
        fieldName: 'eventName',
        stringFilter: { value: 'entity_clicked' }
      }
    };
    
    if (puebloId) {
      filter = {
        filter: {
          andGroup: {
            filters: [
              { fieldName: 'eventName', stringFilter: { value: 'entity_clicked' } },
              { fieldName: 'customEvent:pueblo_id', stringFilter: { value: puebloId } }
            ]
          }
        }
      };
    }
    
    try {
      const [report] = await runReportWithFilter(
        ['customEvent:entity_name', 'customEvent:category_name', 'customEvent:pueblo_id'],
        ['eventCount', 'activeUsers'],
        { startDate, endDate },
        filter,
        'eventCount', 50
      );
      
      entidades = report.rows?.map(row => ({
        entidad: extractValue(row, 0),
        categoria: extractValue(row, 1),
        pueblo: extractValue(row, 2),
        clicks: extractMetric(row, 0),
        users: extractMetric(row, 1),
      })) || [];
    } catch (e) {
      const [eventsReport] = await runReport(['eventName'], ['eventCount'], { startDate, endDate }, 'eventCount', 100);
      
      const entityEvents = eventsReport.rows
        ?.filter(row => extractValue(row, 0).includes('_clicked'))
        .map(row => ({
          entidad: extractValue(row, 0),
          clicks: extractMetric(row, 0),
        })) || [];
      
      entidades = entityEvents;
    }
    
    res.json({ success: true, data: entidades });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// DETALLES DE ENTIDADES (entity_detail_view)
app.get('/api/analytics/entidades-detalles', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    
    let detalles = [];
    
    try {
      const [report] = await runReportWithFilter(
        ['customEvent:entity_name', 'customEvent:category_name', 'customEvent:pueblo_id'],
        ['eventCount', 'activeUsers'],
        { startDate, endDate },
        {
          filter: {
            fieldName: 'eventName',
            stringFilter: { value: 'screen_view' }
          }
        },
        'eventCount', 50
      );
      
      detalles = report.rows
        ?.filter(row => extractValue(row, 0) && extractValue(row, 0) !== '(not set)')
        .map(row => ({
          entidad: extractValue(row, 0),
          categoria: extractValue(row, 1),
          pueblo: extractValue(row, 2),
          views: extractMetric(row, 0),
          users: extractMetric(row, 1),
        })) || [];
    } catch (e) {
      detalles = [];
    }
    
    res.json({ success: true, data: detalles });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// ACCIONES
app.get('/api/analytics/acciones', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    
    let acciones = [];
    
    try {
      const [report] = await runReportWithFilter(
        ['customEvent:action', 'customEvent:entity_name'],
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
      
      acciones = report.rows?.map(row => ({
        action: extractValue(row, 0),
        entidad: extractValue(row, 1),
        count: extractMetric(row, 0),
      })) || [];
    } catch (e) {
      const [eventsReport] = await runReport(['eventName'], ['eventCount'], { startDate, endDate }, 'eventCount', 50);
      
      acciones = eventsReport.rows
        ?.filter(row => extractValue(row, 0).includes('action') || extractValue(row, 0).includes('abrir_'))
        .map(row => ({
          action: extractValue(row, 0).replace('_action', '').replace('abrir_', ''),
          count: extractMetric(row, 0),
        })) || [];
    }
    
    res.json({ success: true, data: acciones });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// RESUMEN DE ACCIONES
app.get('/api/analytics/acciones-resumen', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    
    let resumen = [];
    
    try {
      const [report] = await runReportWithFilter(
        ['customEvent:action'],
        ['eventCount'],
        { startDate, endDate },
        {
          filter: {
            fieldName: 'eventName',
            stringFilter: { value: 'entity_action' }
          }
        },
        'eventCount', 30
      );
      
      resumen = report.rows?.map(row => ({
        action: extractValue(row, 0),
        count: extractMetric(row, 0),
      })) || [];
    } catch (e) {
      resumen = [];
    }
    
    res.json({ success: true, data: resumen });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// MAPAS
app.get('/api/analytics/mapas', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    
    let mapas = [];
    
    try {
      const [puebloReport] = await runReportWithFilter(
        ['customEvent:pueblo_nombre'],
        ['eventCount'],
        { startDate, endDate },
        {
          filter: {
            fieldName: 'eventName',
            stringFilter: { value: 'abrir_mapa_pueblo' }
          }
        },
        'eventCount', 30
      );
      
      mapas.push(...(puebloReport.rows?.map(row => ({
        tipo: 'pueblo',
        lugar: extractValue(row, 0),
        count: extractMetric(row, 0),
      })) || []));
    } catch (e) {}
    
    try {
      const [lugarReport] = await runReportWithFilter(
        ['customEvent:lugar_nombre'],
        ['eventCount'],
        { startDate, endDate },
        {
          filter: {
            fieldName: 'eventName',
            stringFilter: { value: 'abrir_mapa' }
          }
        },
        'eventCount', 30
      );
      
      mapas.push(...(lugarReport.rows?.map(row => ({
        tipo: 'lugar',
        lugar: extractValue(row, 0),
        count: extractMetric(row, 0),
      })) || []));
    } catch (e) {}
    
    res.json({ success: true, data: mapas });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// BÚSQUEDAS
app.get('/api/analytics/busquedas', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    
    let busquedas = [];
    
    try {
      const [report] = await runReportWithFilter(
        ['customEvent:search_term'],
        ['eventCount'],
        { startDate, endDate },
        {
          filter: {
            fieldName: 'eventName',
            stringFilter: { value: 'search' }
          }
        },
        'eventCount', 30
      );
      
      busquedas = report.rows?.map(row => ({
        query: extractValue(row, 0),
        count: extractMetric(row, 0),
      })) || [];
    } catch (e) {
      busquedas = [];
    }
    
    res.json({ success: true, data: busquedas });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// FILTROS
app.get('/api/analytics/filtros', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    
    let filtros = [];
    
    try {
      const [report] = await runReportWithFilter(
        ['customEvent:filter_type', 'customEvent:filter_value'],
        ['eventCount'],
        { startDate, endDate },
        {
          filter: {
            fieldName: 'eventName',
            stringFilter: { value: 'filter' }
          }
        },
        'eventCount', 30
      );
      
      filtros = report.rows?.map(row => ({
        tipo: extractValue(row, 0),
        valor: extractValue(row, 1),
        count: extractMetric(row, 0),
      })) || [];
    } catch (e) {
      filtros = [];
    }
    
    res.json({ success: true, data: filtros });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// SOS
app.get('/api/analytics/sos', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    
    let sos = [];
    
    try {
      const [report] = await runReportWithFilter(
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
      
      sos = report.rows?.map(row => ({
        tipo: extractValue(row, 0),
        action: extractValue(row, 1),
        count: extractMetric(row, 0),
      })) || [];
    } catch (e) {
      sos = [];
    }
    
    res.json({ success: true, data: sos });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// ERRORES
app.get('/api/analytics/errores', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    
    let errores = [];
    
    try {
      const [report] = await runReportWithFilter(
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
      
      errores = report.rows?.map(row => ({
        tipo: extractValue(row, 0),
        mensaje: extractValue(row, 1),
        count: extractMetric(row, 0),
      })) || [];
    } catch (e) {
      errores = [];
    }
    
    res.json({ success: true, data: errores });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// BOTONES
app.get('/api/analytics/botones', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    
    let botones = [];
    
    try {
      const [report] = await runReportWithFilter(
        ['customEvent:item_name', 'customEvent:screen_name'],
        ['eventCount'],
        { startDate, endDate },
        {
          filter: {
            fieldName: 'eventName',
            stringFilter: { value: 'button_clicked' }
          }
        },
        'eventCount', 30
      );
      
      botones = report.rows?.map(row => ({
        boton: extractValue(row, 0),
        pantalla: extractValue(row, 1),
        count: extractMetric(row, 0),
      })) || [];
    } catch (e) {
      botones = [];
    }
    
    res.json({ success: true, data: botones });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// COMPARTIR
app.get('/api/analytics/compartir', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    
    let compartir = [];
    
    try {
      const [report] = await runReportWithFilter(
        ['customEvent:entity_name', 'customEvent:share_method'],
        ['eventCount'],
        { startDate, endDate },
        {
          filter: {
            fieldName: 'eventName',
            stringFilter: { value: 'share' }
          }
        },
        'eventCount', 30
      );
      
      compartir = report.rows?.map(row => ({
        entidad: extractValue(row, 0),
        metodo: extractValue(row, 1),
        count: extractMetric(row, 0),
      })) || [];
    } catch (e) {
      compartir = [];
    }
    
    res.json({ success: true, data: compartir });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// ========================================
// ENDPOINT GENÉRICO DINÁMICO
// ========================================

app.get('/api/analytics/custom', async (req, res) => {
  try {
    const { 
      startDate = '30daysAgo', 
      endDate = 'today',
      eventName,
      dimensions,
      metrics = 'eventCount,activeUsers',
      limit = 30
    } = req.query;
    
    if (!eventName) {
      return res.json({ 
        success: false, 
        error: 'eventName es requerido',
        example: '/api/analytics/custom?eventName=pueblo_view&dimensions=customEvent:pueblo_nombre,customEvent:pueblo_id'
      });
    }
    
    const dimensionsList = dimensions ? dimensions.split(',').map(d => d.trim()) : [];
    const metricsList = metrics.split(',').map(m => m.trim());
    
    const filter = {
      filter: {
        fieldName: 'eventName',
        stringFilter: { value: eventName }
      }
    };
    
    const [report] = await runReportWithFilter(
      dimensionsList,
      metricsList,
      { startDate, endDate },
      filter,
      'eventCount',
      parseInt(limit)
    );
    
    const data = report.rows?.map(row => {
      const obj = {};
      dimensionsList.forEach((dim, idx) => {
        const cleanName = dim.replace('customEvent:', '').replace('firebase:', '');
        obj[cleanName] = extractValue(row, idx);
      });
      metricsList.forEach((metric, idx) => {
        const cleanName = metric === 'eventCount' ? 'count' : 
                         metric === 'activeUsers' ? 'users' : metric;
        obj[cleanName] = extractMetric(row, idx);
      });
      return obj;
    }) || [];
    
    res.json({ 
      success: true, 
      eventName,
      dimensions: dimensionsList,
      metrics: metricsList,
      data 
    });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// ========================================
// DASHBOARD COMPLETO - DINÁMICO (FIREBASE)
// ========================================

app.get('/api/analytics/dashboard', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today', puebloId } = req.query;
    
    const [
      overviewResult,
      pueblosResult,
      categoriasResult,
      entidadesResult,
      accionesResult,
      mapasResult,
      platformsResult,
      countriesResult,
      topEventsResult
    ] = await Promise.allSettled([
      runReport([], ['activeUsers', 'newUsers', 'sessions', 'screenPageViews', 'averageSessionDuration', 'bounceRate', 'engagementRate'], { startDate, endDate }).catch(() => null),
      
      runReportWithFilter(
        ['customEvent:pueblo_nombre'],
        ['eventCount', 'activeUsers'],
        { startDate, endDate },
        { filter: { fieldName: 'eventName', stringFilter: { value: 'pueblo_view' } } },
        'eventCount', 20
      ).catch(() => null),
      
      runReportWithFilter(
        ['customEvent:category_name', 'customEvent:pueblo_id'],
        ['eventCount', 'activeUsers'],
        { startDate, endDate },
        { filter: { fieldName: 'eventName', stringFilter: { value: 'category_clicked' } } },
        'eventCount', 20
      ).catch(() => null),
      
      runReportWithFilter(
        ['customEvent:entity_name', 'customEvent:category_name', 'customEvent:pueblo_id'],
        ['eventCount', 'activeUsers'],
        { startDate, endDate },
        { filter: { fieldName: 'eventName', stringFilter: { value: 'entity_clicked' } } },
        'eventCount', 30
      ).catch(() => null),
      
      runReportWithFilter(
        ['customEvent:action'],
        ['eventCount'],
        { startDate, endDate },
        { filter: { fieldName: 'eventName', stringFilter: { value: 'entity_action' } } },
        'eventCount', 20
      ).catch(() => null),
      
      runReportWithFilter(
        ['customEvent:lugar_nombre'],
        ['eventCount'],
        { startDate, endDate },
        { filter: { fieldName: 'eventName', stringFilter: { matchType: 'CONTAINS', value: 'mapa' } } },
        'eventCount', 20
      ).catch(() => null),
      
      runReport(['platform'], ['activeUsers', 'sessions'], { startDate, endDate }, 'activeUsers', 5).catch(() => null),
      
      runReport(['country'], ['activeUsers'], { startDate, endDate }, 'activeUsers', 10).catch(() => null),
      
      runReport(['eventName'], ['eventCount', 'activeUsers'], { startDate, endDate }, 'eventCount', 30).catch(() => null),
    ]);
    
    const overviewRow = overviewResult?.value?.[0]?.rows?.[0];
    const overview = {
      activeUsers: parseInt(overviewRow?.metricValues?.[0]?.value) || 0,
      newUsers: parseInt(overviewRow?.metricValues?.[1]?.value) || 0,
      sessions: parseInt(overviewRow?.metricValues?.[2]?.value) || 0,
      pageViews: parseInt(overviewRow?.metricValues?.[3]?.value) || 0,
      avgSessionDuration: parseFloat(overviewRow?.metricValues?.[4]?.value) || 0,
      bounceRate: parseFloat(overviewRow?.metricValues?.[5]?.value || 0) * 100,
      engagementRate: parseFloat(overviewRow?.metricValues?.[6]?.value) || 0,
    };
    
    const pueblos = pueblosResult?.value?.[0]?.rows?.map(row => ({
      pueblo: extractValue(row, 0),
      views: extractMetric(row, 0),
      users: extractMetric(row, 1),
    })) || [];
    
    const categorias = categoriasResult?.value?.[0]?.rows?.map(row => ({
      categoria: extractValue(row, 0),
      pueblo: extractValue(row, 1),
      clicks: extractMetric(row, 0),
      users: extractMetric(row, 1),
    })) || [];
    
    let entidades = entidadesResult?.value?.[0]?.rows?.map(row => ({
      entidad: extractValue(row, 0),
      categoria: extractValue(row, 1),
      pueblo: extractValue(row, 2),
      clicks: extractMetric(row, 0),
      users: extractMetric(row, 1),
    })) || [];
    
    if (puebloId) {
      entidades = entidades.filter(e => e.pueblo === puebloId);
    }
    
    const acciones = accionesResult?.value?.[0]?.rows?.map(row => ({
      action: extractValue(row, 0),
      count: extractMetric(row, 0),
    })) || [];
    
    const mapas = mapasResult?.value?.[0]?.rows?.map(row => ({
      lugar: extractValue(row, 0),
      count: extractMetric(row, 0),
    })) || [];
    
    const platforms = platformsResult?.value?.[0]?.rows?.map(row => ({
      platform: extractValue(row, 0),
      users: extractMetric(row, 0),
      sessions: extractMetric(row, 1),
    })) || [];
    
    const countries = countriesResult?.value?.[0]?.rows?.map(row => ({
      country: extractValue(row, 0),
      users: extractMetric(row, 0),
    })) || [];
    
    const topEvents = topEventsResult?.value?.[0]?.rows?.map(row => ({
      event: extractValue(row, 0),
      count: extractMetric(row, 0),
      users: extractMetric(row, 1),
    })) || [];
    
    res.json({
      success: true,
      data: {
        overview,
        pueblos,
        categorias,
        entidades,
        acciones,
        mapas,
        platforms,
        countries,
        topEvents,
        generatedAt: new Date().toISOString()
      }
    });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// ========================================
// PARÁMETROS DISPONIBLES
// ========================================

app.get('/api/analytics/parametros-disponibles', (req, res) => {
  res.json({
    success: true,
    data: {
      eventos: {
        'pueblo_view': {
          descripcion: 'Vista de un pueblo',
          parametros: ['pueblo_id', 'pueblo_nombre', 'screen_name', 'screen_class']
        },
        'category_view': {
          descripcion: 'Vista de una categoría',
          parametros: ['pueblo_id', 'category_id', 'category_name']
        },
        'category_clicked': {
          descripcion: 'Click en una categoría',
          parametros: ['category_id', 'category_name', 'pueblo_id']
        },
        'entity_clicked': {
          descripcion: 'Click en una entidad/lugar',
          parametros: ['entity_id', 'entity_name', 'category_id', 'category_name', 'pueblo_id']
        },
        'entity_action': {
          descripcion: 'Acción sobre una entidad',
          parametros: ['entity_id', 'entity_name', 'category_id', 'pueblo_id', 'action']
        },
        'abrir_mapa_pueblo': {
          descripcion: 'Abrir mapa de un pueblo',
          parametros: ['pueblo_id', 'pueblo_nombre', 'origen']
        },
        'abrir_mapa': {
          descripcion: 'Abrir mapa de un lugar',
          parametros: ['lugar_nombre', 'origen', 'latitud', 'longitud', 'map_url']
        },
        'abrir_informacion': {
          descripcion: 'Abrir información de un lugar',
          parametros: ['lugar_nombre', 'origen', 'info_url']
        },
        'search': {
          descripcion: 'Búsqueda realizada',
          parametros: ['search_term', 'result_count']
        },
        'filter': {
          descripcion: 'Filtro aplicado',
          parametros: ['filter_type', 'filter_value']
        },
        'button_clicked': {
          descripcion: 'Click en botón',
          parametros: ['item_id', 'item_name', 'button_category', 'screen_name']
        },
        'share': {
          descripcion: 'Contenido compartido',
          parametros: ['entity_id', 'entity_name', 'share_method']
        },
        'sos_action': {
          descripcion: 'Acción de emergencia',
          parametros: ['entity_id', 'entity_name', 'sos_type', 'action']
        },
        'app_error': {
          descripcion: 'Error de la aplicación',
          parametros: ['error_type', 'error_message', 'screen_name']
        }
      },
      ejemplo: '/api/analytics/custom?eventName=entity_clicked&dimensions=customEvent:entity_name,customEvent:category_name,customEvent:pueblo_id&metrics=eventCount,activeUsers'
    }
  });
});

// ========================================
// MONGODB ATLAS - ENDPOINTS
// ========================================

// Recibir eventos desde la app Android - MEJORADO Y FLEXIBLE
app.post('/api/mongodb/event', async (req, res) => {
  try {
    if (!mongoDb) {
      return res.json({ 
        success: false, 
        error: 'MongoDB no está conectado',
        note: 'Agrega MONGODB_URI en las variables de entorno de Render'
      });
    }
    
    console.log('📥 Evento recibido:', JSON.stringify(req.body, null, 2));
    
    const { event_name, timestamp, data } = req.body;
    
    if (!event_name) {
      return res.json({ success: false, error: 'event_name es requerido' });
    }
    
    // Normalizar el nombre del evento
    let normalizedEventName = event_name.toLowerCase().trim();
    
    // Crear documento del evento con todos los campos posibles
    const eventDocument = {
      event_name: normalizedEventName,
      timestamp: timestamp || Date.now(),
      server_time: new Date(),
      date: new Date().toISOString().split('T')[0],
      
      // Extraer campos comunes del data para fácil acceso
      pueblo_id: data?.pueblo_id || data?.town_id || null,
      pueblo_nombre: data?.pueblo_nombre || data?.town_name || data?.pueblo || null,
      category_id: data?.category_id || null,
      category_name: data?.category_name || data?.categoria || null,
      entity_id: data?.entity_id || data?.lugar_id || null,
      entity_name: data?.entity_name || data?.lugar_nombre || data?.nombre || null,
      action: data?.action || data?.accion || null,
      
      // Device info
      device_model: data?.device_model || null,
      app_version: data?.app_version || null,
      os_version: data?.os_version || null,
      platform: data?.platform || 'Android',
      
      // Guardar data original completa
      data: data || {}
    };
    
    await mongoDb.collection('events').insertOne(eventDocument);
    
    console.log('✅ Evento guardado:', normalizedEventName, '| Pueblo:', eventDocument.pueblo_nombre, '| Entity:', eventDocument.entity_name);
    
    res.json({ 
      success: true, 
      message: 'Evento guardado en MongoDB',
      event_name: normalizedEventName,
      saved_fields: {
        pueblo: eventDocument.pueblo_nombre,
        category: eventDocument.category_name,
        entity: eventDocument.entity_name,
        action: eventDocument.action
      }
    });
    
  } catch (error) {
    console.error('❌ Error guardando en MongoDB:', error);
    res.json({ success: false, error: error.message });
  }
});

// ========================================
// DASHBOARD MONGODB - FLEXIBLE Y ROBUSTO
// ========================================

app.get('/api/mongodb/dashboard', async (req, res) => {
  try {
    if (!mongoDb) {
      return res.json({ success: false, error: 'MongoDB no está conectado' });
    }
    
    const { days = 7 } = req.query;
    const startDate = new Date();
    startDate.setDate(startDate.getDate() - parseInt(days));

    console.log(`📊 Generando dashboard para últimos ${days} días...`);

    // ========================================
    // CONSULTAS FLEXIBLES - BUSCAN EN MÚLTIPLES CAMPOS
    // ========================================
    
    const [
      totalEvents,
      eventsByType,
      pueblosData,
      categoriasData,
      entidadesData,
      accionesData,
      eventsByDay,
      eventosRecientes
    ] = await Promise.all([
      // 1. Total de eventos
      mongoDb.collection('events').countDocuments({ server_time: { $gte: startDate } }),
      
      // 2. Eventos por tipo
      mongoDb.collection('events').aggregate([
        { $match: { server_time: { $gte: startDate } } },
        { $group: { _id: '$event_name', count: { $sum: 1 } } },
        { $sort: { count: -1 } },
        { $limit: 30 }
      ]).toArray(),
      
      // 3. PUEBLOS - Buscar en múltiples campos y eventos
      mongoDb.collection('events').aggregate([
        { $match: { 
          server_time: { $gte: startDate },
          $or: [
            { pueblo_nombre: { $exists: true, $ne: null, $ne: '' } },
            { 'data.pueblo_nombre': { $exists: true, $ne: null, $ne: '' } },
            { 'data.town_name': { $exists: true, $ne: null, $ne: '' } },
            { pueblo_id: { $exists: true, $ne: null, $ne: '' } }
          ]
        }},
        { $project: {
          pueblo: { 
            $ifNull: [
              '$pueblo_nombre', 
              '$data.pueblo_nombre', 
              '$data.town_name',
              '$pueblo_id',
              '$data.pueblo_id',
              'Sin nombre'
            ]
          }
        }},
        { $match: { pueblo: { $ne: null, $ne: '', $ne: 'Sin nombre' } } },
        { $group: { _id: '$pueblo', count: { $sum: 1 } } },
        { $sort: { count: -1 } },
        { $limit: 20 }
      ]).toArray(),
      
      // 4. CATEGORÍAS - Buscar en múltiples campos
      mongoDb.collection('events').aggregate([
        { $match: { 
          server_time: { $gte: startDate },
          $or: [
            { category_name: { $exists: true, $ne: null, $ne: '' } },
            { 'data.category_name': { $exists: true, $ne: null, $ne: '' } },
            { 'data.categoria': { $exists: true, $ne: null, $ne: '' } }
          ]
        }},
        { $project: {
          categoria: { 
            $ifNull: [
              '$category_name', 
              '$data.category_name',
              '$data.categoria',
              'Sin categoría'
            ]
          },
          pueblo: { 
            $ifNull: [
              '$pueblo_nombre', 
              '$data.pueblo_nombre',
              '$data.town_name',
              null
            ]
          }
        }},
        { $match: { categoria: { $ne: null, $ne: '', $ne: 'Sin categoría' } } },
        { $group: { _id: { categoria: '$categoria', pueblo: '$pueblo' }, count: { $sum: 1 } } },
        { $sort: { count: -1 } },
        { $limit: 30 }
      ]).toArray(),
      
      // 5. ENTIDADES - Buscar en múltiples campos
      mongoDb.collection('events').aggregate([
        { $match: { 
          server_time: { $gte: startDate },
          $or: [
            { entity_name: { $exists: true, $ne: null, $ne: '' } },
            { 'data.entity_name': { $exists: true, $ne: null, $ne: '' } },
            { 'data.lugar_nombre': { $exists: true, $ne: null, $ne: '' } },
            { 'data.nombre': { $exists: true, $ne: null, $ne: '' } }
          ]
        }},
        { $project: {
          entidad: { 
            $ifNull: [
              '$entity_name', 
              '$data.entity_name',
              '$data.lugar_nombre',
              '$data.nombre',
              'Sin nombre'
            ]
          },
          categoria: { 
            $ifNull: [
              '$category_name', 
              '$data.category_name',
              null
            ]
          },
          pueblo: { 
            $ifNull: [
              '$pueblo_nombre', 
              '$data.pueblo_nombre',
              '$data.town_name',
              null
            ]
          }
        }},
        { $match: { entidad: { $ne: null, $ne: '', $ne: 'Sin nombre' } } },
        { $group: { _id: { entidad: '$entidad', categoria: '$categoria', pueblo: '$pueblo' }, count: { $sum: 1 } } },
        { $sort: { count: -1 } },
        { $limit: 50 }
      ]).toArray(),
      
      // 6. ACCIONES - Buscar en múltiples campos
      mongoDb.collection('events').aggregate([
        { $match: { 
          server_time: { $gte: startDate },
          $or: [
            { action: { $exists: true, $ne: null, $ne: '' } },
            { 'data.action': { $exists: true, $ne: null, $ne: '' } },
            { 'data.accion': { $exists: true, $ne: null, $ne: '' } },
            { event_name: { $regex: /action|llamar|whatsapp|mapa|web|social|share/i } }
          ]
        }},
        { $project: {
          action: { 
            $ifNull: [
              '$action', 
              '$data.action',
              '$data.accion',
              '$event_name'
            ]
          },
          entity: { 
            $ifNull: [
              '$entity_name', 
              '$data.entity_name',
              '$data.lugar_nombre',
              null
            ]
          },
          pueblo: { 
            $ifNull: [
              '$pueblo_nombre', 
              '$data.pueblo_nombre',
              '$data.town_name',
              null
            ]
          }
        }},
        { $match: { action: { $ne: null, $ne: '' } } },
        { $group: { _id: { action: '$action', entity: '$entity', pueblo: '$pueblo' }, count: { $sum: 1 } } },
        { $sort: { count: -1 } },
        { $limit: 50 }
      ]).toArray(),
      
      // 7. Eventos por día
      mongoDb.collection('events').aggregate([
        { $match: { server_time: { $gte: startDate } } },
        { $group: { _id: '$date', count: { $sum: 1 } } },
        { $project: { _id: 0, date: '$_id', count: 1 } },
        { $sort: { date: 1 } }
      ]).toArray(),
      
      // 8. Últimos 5 eventos para debug
      mongoDb.collection('events')
        .find({ server_time: { $gte: startDate } })
        .sort({ server_time: -1 })
        .limit(5)
        .toArray()
    ]);

    console.log(`📊 Total eventos: ${totalEvents}`);
    console.log(`📊 Pueblos encontrados: ${pueblosData.length}`);
    console.log(`📊 Categorías encontradas: ${categoriasData.length}`);
    console.log(`📊 Entidades encontradas: ${entidadesData.length}`);
    console.log(`📊 Acciones encontradas: ${accionesData.length}`);

    // ========================================
    // CONSTRUIR RESPUESTA
    // ========================================
    
    res.json({
      success: true,
      data: {
        // Resumen
        totalEvents: totalEvents,
        resumen: {
          totalEventos: totalEvents,
          totalPueblos: pueblosData.length,
          totalCategorias: categoriasData.length,
          totalEntidades: entidadesData.length,
          totalAcciones: accionesData.reduce((sum, a) => sum + a.count, 0),
          periodo: `Últimos ${days} días`
        },
        
        // Datos planos
        eventosPorTipo: eventsByType.map(e => ({ evento: e._id, count: e.count })),
        topPueblos: pueblosData.map(p => ({ pueblo: p._id, count: p.count })),
        topCategorias: categoriasData.map(c => ({ 
          categoria: c._id.categoria, 
          pueblo: c._id.pueblo,
          count: c.count 
        })),
        topEntidades: entidadesData.map(e => ({ 
          entidad: e._id.entidad, 
          categoria: e._id.categoria,
          pueblo: e._id.pueblo,
          count: e.count 
        })),
        acciones: accionesData.map(a => ({ 
          action: a._id.action, 
          entity: a._id.entity,
          pueblo: a._id.pueblo,
          count: a.count 
        })),
        eventsByDay: eventsByDay,
        
        // Para debug
        ultimosEventos: eventosRecientes.map(e => ({
          event_name: e.event_name,
          pueblo: e.pueblo_nombre || e.data?.pueblo_nombre || e.data?.town_name,
          entity: e.entity_name || e.data?.entity_name || e.data?.lugar_nombre,
          action: e.action || e.data?.action,
          server_time: e.server_time
        }))
      }
    });
    
  } catch (error) {
    console.error('❌ Error en dashboard MongoDB:', error);
    res.json({ success: false, error: error.message });
  }
});

// Endpoint para ver eventos recientes (debug)
app.get('/api/mongodb/events/recent', async (req, res) => {
  try {
    if (!mongoDb) {
      return res.json({ success: false, error: 'MongoDB no está conectado' });
    }
    
    const { limit = 30 } = req.query;
    
    const events = await mongoDb.collection('events')
      .find({})
      .sort({ server_time: -1 })
      .limit(parseInt(limit))
      .toArray();
    
    res.json({ success: true, count: events.length, events });
    
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// Endpoint para ver TODOS los tipos de eventos disponibles
app.get('/api/mongodb/events/types', async (req, res) => {
  try {
    if (!mongoDb) {
      return res.json({ success: false, error: 'MongoDB no está conectado' });
    }
    
    const types = await mongoDb.collection('events').aggregate([
      { $group: { 
        _id: '$event_name', 
        count: { $sum: 1 },
        sample: { $first: '$$ROOT' }
      }},
      { $sort: { count: -1 } }
    ]).toArray();
    
    res.json({ 
      success: true, 
      totalTypes: types.length,
      types: types.map(t => ({
        event_name: t._id,
        count: t.count,
        sample_data: t.sample?.data || t.sample
      }))
    });
    
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// ========================================
// INICIAR SERVIDOR
// ========================================

app.listen(PORT, () => {
  console.log(`🚀 Turisteando Analytics Server v4.0 (Flexible) running on port ${PORT}`);
  console.log(`📊 Firebase Property ID: ${PROPERTY_ID}`);
  console.log(`🍃 MongoDB: ${mongoDb ? 'Conectado' : 'No configurado'}`);
  console.log(`🔗 Health check: http://localhost:${PORT}/api/health`);
});
