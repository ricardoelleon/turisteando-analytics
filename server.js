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
    version: '2.2.0-optimized' // Versión actualizada
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
// DASHBOARD COMPLETO - DINÁMICO
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
// MONGODB ATLAS - NUEVOS ENDPOINTS
// ========================================

// Recibir eventos desde la app Android
app.post('/api/mongodb/event', async (req, res) => {
  try {
    if (!mongoDb) {
      return res.json({ 
        success: false, 
        error: 'MongoDB no está conectado',
        note: 'Agrega MONGODB_URI en las variables de entorno de Render'
      });
    }
    
    const { event_name, timestamp, data } = req.body;
    
    if (!event_name) {
      return res.json({ success: false, error: 'event_name es requerido' });
    }
    
    const eventDocument = {
      event_name,
      timestamp: timestamp || Date.now(),
      data: data || {},
      server_time: new Date(),
      date: new Date().toISOString().split('T')[0]
    };
    
    await mongoDb.collection('events').insertOne(eventDocument);
    
    res.json({ 
      success: true, 
      message: 'Evento guardado en MongoDB',
      event_name 
    });
    
  } catch (error) {
    console.error('Error guardando en MongoDB:', error);
    res.json({ success: false, error: error.message });
  }
});

// ========================================
// DASHBOARD MONGODB - OPTIMIZADO (PROMISE.ALL)
// ========================================
// ========================================
// DASHBOARD MONGODB - CORREGIDO (AGRUPA POR PUEBLO)
// ========================================

app.get('/api/mongodb/dashboard', async (req, res) => {
  try {
    if (!mongoDb) {
      return res.json({ 
        success: false, 
        error: 'MongoDB no está conectado' 
      });
    }
    
    const { days = 7 } = req.query;
    const startDate = new Date();
    startDate.setDate(startDate.getDate() - parseInt(days));

    // Helper para obtener el nombre del pueblo de forma segura
    // Busca pueblo_nombre, si no existe busca pueblo_id
       const getPuebloField = { $ifNull: ['$data.pueblo_nombre', '$data.pueblo_id', '$data.origen'] };

    // EJECUTAR TODAS LAS CONSULTAS EN PARALELO
    const [
      totalEvents,
      eventsByType,
      topPueblos,
      topCategorias,
      topEntidades,
      acciones,
      eventsByDay
    ] = await Promise.all([
      // 1. Total Eventos
      mongoDb.collection('events').countDocuments({ server_time: { $gte: startDate } }),
      
      // 2. Eventos por Tipo
      mongoDb.collection('events').aggregate([
        { $match: { server_time: { $gte: startDate } } },
        { $group: { _id: '$event_name', count: { $sum: 1 } } },
        { $sort: { count: -1 } },
        { $limit: 20 }
      ]).toArray(),
      
      // 3. Top Pueblos (Agrupa por pueblo_nombre o pueblo_id)
      mongoDb.collection('events').aggregate([
        { $match: { server_time: { $gte: startDate } } },
        { $group: { _id: getPuebloField, count: { $sum: 1 } } },
        { $match: { _id: { $ne: null, $ne: "" } } }, // Filtra nulos
        { $sort: { count: -1 } },
        { $limit: 15 }
      ]).toArray(),
      
      // 4. Top Categorías (CORREGIDO: Agrupa por Categoría Y Pueblo)
      mongoDb.collection('events').aggregate([
        { $match: { server_time: { $gte: startDate }, event_name: { $in: ['category_click', 'category_view'] } } },
        { $group: { 
            _id: { 
              categoria: '$data.category_name', 
              pueblo: getPuebloField 
            }, 
            count: { $sum: 1 } 
        }},
        { $sort: { count: -1 } },
        { $limit: 20 }
      ]).toArray(),
      
      // 5. Top Entidades (CORREGIDO: Agrupa por Entidad, Categoría Y Pueblo)
      mongoDb.collection('events').aggregate([
        { $match: { server_time: { $gte: startDate }, event_name: 'entity_clicked' } },
        { $group: { 
            _id: { 
              nombre: '$data.entity_name', 
              categoria: '$data.category_name', 
              pueblo: getPuebloField 
            }, 
            count: { $sum: 1 } 
        }},
        { $sort: { count: -1 } },
        { $limit: 20 }
      ]).toArray(),
      
      // 6. Acciones (CORREGIDO: Agrupa por Acción, Entidad Y Pueblo)
      mongoDb.collection('events').aggregate([
        { $match: { server_time: { $gte: startDate }, event_name: 'entity_action' } },
        { $group: { 
            _id: { 
              action: '$data.action', 
              entity: '$data.entity_name', 
              pueblo: getPuebloField 
            }, 
            count: { $sum: 1 } 
        }},
        { $sort: { count: -1 } },
        { $limit: 20 }
      ]).toArray(),
      
      // 7. Eventos por Día
      mongoDb.collection('events').aggregate([
        { $match: { server_time: { $gte: startDate } } },
        { $group: { _id: '$date', count: { $sum: 1 }, unique_users: { $addToSet: '$data.device_model' } } },
        { $project: { _id: 0, date: '$_id', count: 1, devices: { $size: '$unique_users' } } },
        { $sort: { date: 1 } }
      ]).toArray()
    ]);
    
    res.json({
      success: true,
      data: {
        totalEvents,
        eventsByType: eventsByType.map(e => ({ event: e._id, count: e.count })),
        topPueblos: topPueblos.map(p => ({ pueblo: p._id, count: p.count })),
        
        // Mapeos actualizados para incluir 'pueblo'
        topCategorias: topCategorias.filter(c => c._id.categoria).map(c => ({ 
          categoria: c._id.categoria, 
          pueblo: c._id.pueblo || 'Sin pueblo',
          count: c.count 
        })),
        
        topEntidades: topEntidades.filter(e => e._id.nombre).map(e => ({ 
          entidad: e._id.nombre, 
          categoria: e._id.categoria, 
          pueblo: e._id.pueblo || 'Sin pueblo',
          count: e.count 
        })),
        
        acciones: acciones.filter(a => a._id.action).map(a => ({ 
          action: a._id.action, 
          entity: a._id.entity,
          pueblo: a._id.pueblo || 'Sin pueblo',
          count: a.count 
        })),
        
        eventsByDay
      }
    });
    
  } catch (error) {
    console.error('Error en dashboard MongoDB:', error);
    res.json({ success: false, error: error.message });
  }
});
// ========================================
// INICIAR SERVIDOR
// ========================================

app.listen(PORT, () => {
  console.log(`🚀 Turisteando Analytics Server v2.2 (Optimized) running on port ${PORT}`);
  console.log(`📊 Firebase Property ID: ${PROPERTY_ID}`);
  console.log(`🍃 MongoDB: ${mongoDb ? 'Conectado' : 'No configurado'}`);
  console.log(`🔗 Health check: http://localhost:${PORT}/api/health`);
});
