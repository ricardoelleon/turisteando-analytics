const express = require('express');
const cors = require('cors');
const { BetaAnalyticsDataClient } = require('@google-analytics/data');
const { MongoClient } = require('mongodb');
// ========================================
// NUEVO: Firebase Admin para notificaciones
// ========================================
const admin = require('firebase-admin');

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
// NUEVO: Firebase Admin para notificaciones push
// ========================================
function initializeFirebaseAdmin() {
  try {
    if (!admin.apps.length) {
      admin.initializeApp({
        credential: admin.credential.cert({
          projectId: process.env.PROPERTY_ID || 'turisteando-app',
          clientEmail: process.env.SERVICE_ACCOUNT_EMAIL,
          privateKey: process.env.SERVICE_ACCOUNT_PRIVATE_KEY.replace(/\\n/g, '\n'),
        }),
      });
      console.log('✅ Firebase Admin inicializado para notificaciones');
    }
    return true;
  } catch (error) {
    console.error('❌ Error inicializando Firebase Admin:', error.message);
    return false;
  }
}

// Inicializar después de conectar MongoDB
initializeFirebaseAdmin();

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

function extractValue(row, index, defaultValue = '') {
  return row.dimensionValues?.[index]?.value || defaultValue;
}

function extractMetric(row, index, defaultValue = 0) {
  return parseInt(row.metricValues?.[index]?.value) || defaultValue;
}

// ========================================
// FUNCIÓN INTELIGENTE - VOTO MAYORITARIO
// ========================================

/**
 * Calcula la categoría correcta para cada entidad usando voto mayoritario.
 * Una entidad se asigna a la categoría donde tiene MÁS eventos.
 */
function calculateCorrectCategories(entidadesData) {
  // Mapa para agrupar eventos por entidad
  const entidadStats = new Map();
  
  entidadesData.forEach(e => {
    const entidadNombre = e._id.entidad;
    const categoria = e._id.categoria || 'sin_categoria';
    const pueblo = e._id.pueblo;
    const clicks = e.clicks;
    
    if (!entidadNombre) return;
    
    // Clave única para esta entidad en este pueblo
    const entityKey = `${entidadNombre}|${pueblo || 'sin_pueblo'}`;
    
    if (!entidadStats.has(entityKey)) {
      entidadStats.set(entityKey, {
        nombre: entidadNombre,
        pueblo: pueblo,
        categorias: new Map(),
        totalClicks: 0
      });
    }
    
    const stats = entidadStats.get(entityKey);
    stats.totalClicks += clicks;
    
    // Contar eventos por categoría
    const currentCount = stats.categorias.get(categoria) || 0;
    stats.categorias.set(categoria, currentCount + clicks);
  });
  
  // Determinar la categoría ganadora para cada entidad
  const corrections = new Map();
  
  entidadStats.forEach((stats, entityKey) => {
    let maxCount = 0;
    let winningCategory = null;
    
    stats.categorias.forEach((count, categoria) => {
      if (count > maxCount) {
        maxCount = count;
        winningCategory = categoria;
      }
    });
    
    corrections.set(entityKey, {
      categoriaCorrecta: winningCategory,
      totalEventos: stats.totalClicks,
      detalles: Object.fromEntries(stats.categorias)
    });
  });
  
  return corrections;
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
    firebaseAdmin: admin.apps.length > 0 ? 'initialized' : 'not initialized',
    version: '4.0.0-notifications'
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
// DASHBOARD MONGODB - JERÁRQUICO INTELIGENTE
// ========================================

app.get('/api/mongodb/dashboard', async (req, res) => {
  try {
    if (!mongoDb) {
      return res.json({ success: false, error: 'MongoDB no está conectado' });
    }
    
    const { days = 7 } = req.query;
    const startDate = new Date();
    startDate.setDate(startDate.getDate() - parseInt(days));

    // ========================================
    // PASO 1: OBTENER TODOS LOS DATOS CRUDOS
    // ========================================
    
    const [
      totalEvents,
      eventsByType,
      pueblosViews,
      categoriasData,
      entidadesData,
      accionesData,
      eventsByDay
    ] = await Promise.all([
      // 1. Total de eventos
      mongoDb.collection('events').countDocuments({ server_time: { $gte: startDate } }),
      
      // 2. Eventos por tipo
      mongoDb.collection('events').aggregate([
        { $match: { server_time: { $gte: startDate } } },
        { $group: { _id: '$event_name', count: { $sum: 1 } } },
        { $sort: { count: -1 } },
        { $limit: 20 }
      ]).toArray(),
      
      // 3. Vistas de pueblos - NORMALIZADO A MINÚSCULAS
      mongoDb.collection('events').aggregate([
        { $match: { 
          server_time: { $gte: startDate },
          $or: [
            { event_name: 'pueblo_view' },
            { event_name: 'screen_view', 'data.screen_category': 'town' },
            { event_name: 'abrir_mapa_pueblo' }
          ]
        }},
        { $project: {
          pueblo: { 
            $toLower: { 
              $ifNull: ['$data.pueblo_nombre', '$data.town_name', '$data.pueblo_id', '$data.screen_name'] 
            }
          }
        }},
        { $match: { pueblo: { $ne: null, $ne: '' } } },
        { $group: { _id: '$pueblo', views: { $sum: 1 } } },
        { $sort: { views: -1 } },
        { $limit: 20 }
      ]).toArray(),
      
      // 4. Categorías con pueblo - NORMALIZADO
      mongoDb.collection('events').aggregate([
        { $match: { 
          server_time: { $gte: startDate },
          event_name: { $in: ['category_view', 'category_click'] }
        }},
        { $project: {
          categoria: { $toLower: { $ifNull: ['$data.category_name', '$data.category_id'] } },
          pueblo: { $toLower: { $ifNull: ['$data.pueblo_nombre', '$data.pueblo_id'] } }
        }},
        { $match: { categoria: { $ne: null, $ne: '' } } },
        { $group: { _id: { categoria: '$categoria', pueblo: '$pueblo' }, count: { $sum: 1 } } },
        { $sort: { count: -1 } },
        { $limit: 30 }
      ]).toArray(),
      
      // 5. Entidades con categoría y pueblo - NORMALIZADO
      mongoDb.collection('events').aggregate([
        { $match: { 
          server_time: { $gte: startDate },
          event_name: { $in: ['entity_clicked', 'entity_detail_view'] }
        }},
        { $project: {
          entidad: '$data.entity_name',
          categoria: { $toLower: { $ifNull: ['$data.category_name', '$data.category_id'] } },
          pueblo: { $toLower: { $ifNull: ['$data.pueblo_nombre', '$data.pueblo_id'] } }
        }},
        { $match: { entidad: { $ne: null, $ne: '' } } },
        { $group: { _id: { entidad: '$entidad', categoria: '$categoria', pueblo: '$pueblo' }, clicks: { $sum: 1 } } },
        { $sort: { clicks: -1 } },
        { $limit: 50 }
      ]).toArray(),
      
      // 6. Acciones con entidad, categoría y pueblo - NORMALIZADO
      mongoDb.collection('events').aggregate([
        { $match: { 
          server_time: { $gte: startDate },
          event_name: { $in: ['entity_action', 'abrir_mapa', 'abrir_informacion', 'social_network_open', 'whatsapp_open', 'phone_call', 'share'] }
        }},
        { $project: {
          action: {
            $switch: {
              branches: [
                { case: { $eq: ['$event_name', 'abrir_mapa'] }, then: 'abrir_mapa' },
                { case: { $eq: ['$event_name', 'abrir_informacion'] }, then: 'abrir_web' },
                { case: { $eq: ['$event_name', 'social_network_open'] }, then: { $concat: ['ver_', { $toLower: '$data.network' }] } },
                { case: { $eq: ['$event_name', 'whatsapp_open'] }, then: 'whatsapp' },
                { case: { $eq: ['$event_name', 'phone_call'] }, then: 'llamar' },
                { case: { $eq: ['$event_name', 'share'] }, then: 'compartir' }
              ],
              default: { $toLower: '$data.action' }
            }
          },
          entity: '$data.entity_name',
          categoria: { $toLower: { $ifNull: ['$data.category_name', '$data.category_id'] } },
          pueblo: { $toLower: { $ifNull: ['$data.pueblo_nombre', '$data.pueblo_id', '$data.origen'] } }
        }},
        { $match: { action: { $ne: null, $ne: '' }, entity: { $ne: null, $ne: '' } } },
        { $group: { _id: { action: '$action', entity: '$entity', categoria: '$categoria', pueblo: '$pueblo' }, count: { $sum: 1 } } },
        { $sort: { count: -1 } },
        { $limit: 100 }
      ]).toArray(),
      
      // 7. Eventos por día
      mongoDb.collection('events').aggregate([
        { $match: { server_time: { $gte: startDate } } },
        { $group: { _id: '$date', count: { $sum: 1 }, devices: { $addToSet: '$data.device_model' } } },
        { $project: { _id: 0, date: '$_id', count: 1, devices: { $size: '$devices' } } },
        { $sort: { date: 1 } }
      ]).toArray()
    ]);

    // ========================================
    // PASO 2: APLICAR VOTO MAYORITARIO
    // ========================================
    
    const categoryCorrections = calculateCorrectCategories(entidadesData);

    // ========================================
    // PASO 3: CONSTRUIR ESTRUCTURA JERÁRQUICA
    // ========================================
    
    const pueblosMap = new Map();
    
    // Función helper para normalizar nombres de pueblo
    const normalizePueblo = (nombre) => {
      if (!nombre) return 'sin_pueblo';
      const normalized = nombre.toString().toLowerCase().trim();
      // Mapear variaciones conocidas
      const mappings = {
        'villa de leyva': 'villa_de_leyva',
        'villa_de_leyva': 'villa_de_leyva',
        'raquira': 'raquira',
        'ráquira': 'raquira',
        'tinjaca': 'tinjaca',
        'tinjacá': 'tinjaca',
        'sutamarchan': 'sutamarchan',
        'sutamarchán': 'sutamarchan',
        'susa': 'susa',
        'briceño': 'briceno',
        'briceno': 'briceno'
      };
      return mappings[normalized] || normalized.replace(/\s+/g, '_');
    };
    
    // Función helper para nombre display
    const formatPuebloName = (normalized) => {
      const displayNames = {
        'raquira': 'Ráquira',
        'tinjaca': 'Tinjacá',
        'sutamarchan': 'Sutamarchán',
        'villa_de_leyva': 'Villa de Leyva',
        'susa': 'Susa',
        'briceno': 'Briceño',
        'sin_pueblo': 'Sin Pueblo'
      };
      return displayNames[normalized] || normalized.charAt(0).toUpperCase() + normalized.slice(1).replace(/_/g, ' ');
    };
    
    // Inicializar pueblos desde vistas
    pueblosViews.forEach(p => {
      if (p._id) {
        const puebloKey = normalizePueblo(p._id);
        if (!pueblosMap.has(puebloKey)) {
          pueblosMap.set(puebloKey, {
            nombre: puebloKey,
            displayName: formatPuebloName(puebloKey),
            vistas: p.views,
            categorias: new Map(),
            entidades: new Map()
          });
        } else {
          pueblosMap.get(puebloKey).vistas += p.views;
        }
      }
    });
    
    // Agregar categorías a pueblos
    categoriasData.forEach(c => {
      const puebloKey = normalizePueblo(c._id.pueblo);
      const categoriaNombre = c._id.categoria || 'sin_categoria';
      
      if (!pueblosMap.has(puebloKey)) {
        pueblosMap.set(puebloKey, {
          nombre: puebloKey,
          displayName: formatPuebloName(puebloKey),
          vistas: 0,
          categorias: new Map(),
          entidades: new Map()
        });
      }
      
      const pueblo = pueblosMap.get(puebloKey);
      
      if (!pueblo.categorias.has(categoriaNombre)) {
        pueblo.categorias.set(categoriaNombre, {
          nombre: categoriaNombre,
          vistas: 0,
          entidades: new Map()
        });
      }
      pueblo.categorias.get(categoriaNombre).vistas += c.count;
    });
    
    // Agregar entidades a pueblos y categorías - USANDO CATEGORÍA CORREGIDA
    entidadesData.forEach(e => {
      const puebloKey = normalizePueblo(e._id.pueblo);
      const categoriaOriginal = e._id.categoria || 'sin_categoria';
      const entidadNombre = e._id.entidad;
      
      if (!entidadNombre) return;
      
      // ===== AQUÍ ESTÁ LA MAGIA INTELIGENTE =====
      // Obtener la categoría correcta por voto mayoritario
      const entityKey = `${entidadNombre}|${puebloKey}`;
      const correction = categoryCorrections.get(entityKey);
      const categoriaNombre = correction ? correction.categoriaCorrecta : categoriaOriginal;
      // ===========================================
      
      if (!pueblosMap.has(puebloKey)) {
        pueblosMap.set(puebloKey, {
          nombre: puebloKey,
          displayName: formatPuebloName(puebloKey),
          vistas: 0,
          categorias: new Map(),
          entidades: new Map()
        });
      }
      
      const pueblo = pueblosMap.get(puebloKey);
      
      // Usar solo la clave de entidad (sin categoría) para evitar duplicados
      const entidadKey = entidadNombre;
      
      // Agregar a entidades del pueblo
      if (!pueblo.entidades.has(entidadKey)) {
        pueblo.entidades.set(entidadKey, {
          nombre: entidadNombre,
          categoria: categoriaNombre,
          clicks: 0,
          acciones: []
        });
      }
      pueblo.entidades.get(entidadKey).clicks += e.clicks;
      
      // Agregar a la categoría del pueblo
      if (!pueblo.categorias.has(categoriaNombre)) {
        pueblo.categorias.set(categoriaNombre, {
          nombre: categoriaNombre,
          vistas: 0,
          entidades: new Map()
        });
      }
      
      const categoria = pueblo.categorias.get(categoriaNombre);
      if (!categoria.entidades.has(entidadKey)) {
        categoria.entidades.set(entidadKey, {
          nombre: entidadNombre,
          clicks: 0,
          acciones: []
        });
      }
      categoria.entidades.get(entidadKey).clicks += e.clicks;
    });
    
    // Agregar acciones a entidades - TAMBIÉN USAR CATEGORÍA CORREGIDA
    accionesData.forEach(a => {
      const puebloKey = normalizePueblo(a._id.pueblo);
      const entidadNombre = a._id.entity;
      const categoriaOriginal = a._id.categoria || 'sin_categoria';
      const actionName = a._id.action;
      
      if (!entidadNombre || !actionName) return;
      
      // Obtener categoría corregida
      const entityKey = `${entidadNombre}|${puebloKey}`;
      const correction = categoryCorrections.get(entityKey);
      const categoriaNombre = correction ? correction.categoriaCorrecta : categoriaOriginal;
      
      if (pueblosMap.has(puebloKey)) {
        const pueblo = pueblosMap.get(puebloKey);
        const entidadKey = entidadNombre;
        
        // Buscar la entidad en el pueblo
        if (pueblo.entidades.has(entidadKey)) {
          const entidad = pueblo.entidades.get(entidadKey);
          // Verificar si ya existe esta acción
          const accionExistente = entidad.acciones.find(acc => acc.accion === actionName);
          if (accionExistente) {
            accionExistente.count += a.count;
          } else {
            entidad.acciones.push({
              accion: actionName,
              count: a.count
            });
          }
        }
        
        // También actualizar en la categoría
        if (pueblo.categorias.has(categoriaNombre)) {
          const categoria = pueblo.categorias.get(categoriaNombre);
          if (categoria.entidades.has(entidadKey)) {
            const entidad = categoria.entidades.get(entidadKey);
            const accionExistente = entidad.acciones.find(acc => acc.accion === actionName);
            if (accionExistente) {
              accionExistente.count += a.count;
            } else {
              entidad.acciones.push({
                accion: actionName,
                count: a.count
              });
            }
          }
        }
      }
    });
    
    // ========================================
    // PASO 4: CONVERTIR MAPS A ARRAYS
    // ========================================
    
    const pueblosJerarquico = Array.from(pueblosMap.values())
      .filter(p => p.vistas > 0 || p.entidades.size > 0 || p.categorias.size > 0)
      .map(p => ({
        nombre: p.displayName,
        id: p.nombre,
        vistas: p.vistas,
        categorias: Array.from(p.categorias.values())
          .filter(c => c.vistas > 0 || c.entidades.size > 0)
          .sort((a, b) => b.vistas - a.vistas)
          .map(c => ({
            nombre: c.nombre.charAt(0).toUpperCase() + c.nombre.slice(1).replace(/_/g, ' '),
            vistas: c.vistas,
            entidades: Array.from(c.entidades.values())
              .sort((a, b) => b.clicks - a.clicks)
              .map(e => ({
                nombre: e.nombre,
                clicks: e.clicks,
                acciones: e.acciones.sort((a, b) => b.count - a.count)
              }))
          })),
        entidades: Array.from(p.entidades.values())
          .sort((a, b) => b.clicks - a.clicks)
          .map(e => ({
            nombre: e.nombre,
            categoria: e.categoria,
            clicks: e.clicks,
            acciones: e.acciones.sort((a, b) => b.count - a.count)
          }))
      }))
      .sort((a, b) => b.vistas - a.vistas);

    // ========================================
    // PASO 5: CALCULAR RESUMEN
    // ========================================
    
    const totalCategorias = new Set();
    const totalEntidades = new Set();
    
    pueblosJerarquico.forEach(p => {
      p.categorias.forEach(c => {
        totalCategorias.add(c.nombre);
        c.entidades.forEach(e => {
          totalEntidades.add(e.nombre);
        });
      });
      p.entidades.forEach(e => {
        totalEntidades.add(e.nombre);
      });
    });

    // ========================================
    // PASO 6: RESPUESTA
    // ========================================
    
    res.json({
      success: true,
      data: {
        // Resumen
        resumen: {
          totalEventos: totalEvents,
          totalPueblos: pueblosJerarquico.length,
          totalCategorias: totalCategorias.size,
          totalEntidades: totalEntidades.size,
          totalAcciones: accionesData.reduce((sum, a) => sum + a.count, 0),
          periodo: `Últimos ${days} días`,
          modoInteligente: true
        },
        
        // Estructura jerárquica
        pueblos: pueblosJerarquico,
        
        // Datos planos (para compatibilidad)
        eventosPorTipo: eventsByType.map(e => ({ evento: e._id, count: e.count })),
        topPueblos: pueblosViews.map(p => ({ pueblo: p._id, count: p.views })),
        topCategorias: categoriasData.map(c => ({ 
          categoria: c._id.categoria, 
          pueblo: c._id.pueblo,
          count: c.count 
        })),
        topEntidades: entidadesData.map(e => ({ 
          entidad: e._id.entidad, 
          categoria: e._id.categoria,
          pueblo: e._id.pueblo,
          count: e.clicks 
        })),
        acciones: accionesData.map(a => ({ 
          action: a._id.action, 
          entity: a._id.entity,
          categoria: a._id.categoria,
          pueblo: a._id.pueblo,
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

// ========================================
// ENDPOINT PARA VER CORRECCIONES (DEBUG)
// ========================================

app.get('/api/mongodb/debug/corrections', async (req, res) => {
  try {
    if (!mongoDb) {
      return res.json({ success: false, error: 'MongoDB no está conectado' });
    }
    
    const { days = 7 } = req.query;
    const startDate = new Date();
    startDate.setDate(startDate.getDate() - parseInt(days));
    
    // Obtener datos de entidades
    const entidadesData = await mongoDb.collection('events').aggregate([
      { $match: { 
        server_time: { $gte: startDate },
        event_name: { $in: ['entity_clicked', 'entity_detail_view'] }
      }},
      { $project: {
        entidad: '$data.entity_name',
        categoria: { $toLower: { $ifNull: ['$data.category_name', '$data.category_id'] } },
        pueblo: { $toLower: { $ifNull: ['$data.pueblo_nombre', '$data.pueblo_id'] } }
      }},
      { $match: { entidad: { $ne: null, $ne: '' } } },
      { $group: { _id: { entidad: '$entidad', categoria: '$categoria', pueblo: '$pueblo' }, clicks: { $sum: 1 } } },
      { $sort: { clicks: -1 } }
    ]).toArray();
    
    // Calcular correcciones
    const corrections = calculateCorrectCategories(entidadesData);
    
    // Formatear resultado
    const listaCorrecciones = [];
    corrections.forEach((info, entityKey) => {
      const [nombre, pueblo] = entityKey.split('|');
      const categoriasArray = Object.entries(info.detalles).map(([cat, count]) => ({
        categoria: cat,
        eventos: count
      })).sort((a, b) => b.eventos - a.eventos);
      
      // Solo mostrar si hay más de una categoría (es decir, había conflicto)
      if (categoriasArray.length > 1) {
        listaCorrecciones.push({
          entidad: nombre,
          pueblo: pueblo,
          categoriaGanadora: info.categoriaCorrecta,
          totalEventos: info.totalEventos,
          distribucion: categoriasArray
        });
      }
    });
    
    // Ordenar por más eventos
    listaCorrecciones.sort((a, b) => b.totalEventos - a.totalEventos);
    
    res.json({
      success: true,
      resumen: {
        totalEntidades: corrections.size,
        entidadesConConflicto: listaCorrecciones.length,
        periodo: `Últimos ${days} días`
      },
      correcciones: listaCorrecciones
    });
    
  } catch (error) {
    console.error('Error en debug corrections:', error);
    res.json({ success: false, error: error.message });
  }
});

// ========================================
// ========================================
// NUEVO: NOTIFICACIONES PUSH - ENDPOINTS
// ========================================
// ========================================

/**
 * Registrar token de dispositivo
 * POST /api/notifications/register
 */
app.post('/api/notifications/register', async (req, res) => {
  try {
    if (!mongoDb) {
      return res.json({ success: false, error: 'MongoDB no está conectado' });
    }
    
    const { token, user_id, pueblo_preferido, categorias_interes } = req.body;
    
    if (!token) {
      return res.json({ success: false, error: 'token es requerido' });
    }
    
    const deviceData = {
      token,
      user_id: user_id || null,
      pueblo_preferido: pueblo_preferido || null,
      categorias_interes: categorias_interes || [],
      ultima_actividad: new Date().toISOString().split('T')[0],
      updated_at: new Date()
    };
    
    // Upsert: actualizar si existe, crear si no
    await mongoDb.collection('device_tokens').updateOne(
      { token },
      { $set: deviceData, $setOnInsert: { created_at: new Date() } },
      { upsert: true }
    );
    
    res.json({ success: true, message: 'Token registrado correctamente' });
    
  } catch (error) {
    console.error('Error registrando token:', error);
    res.json({ success: false, error: error.message });
  }
});

/**
 * Obtener datos dinámicos para el panel (pueblos, categorías, entidades)
 * GET /api/notifications/dynamic-data
 */
app.get('/api/notifications/dynamic-data', async (req, res) => {
  try {
    if (!mongoDb) {
      return res.json({ success: false, error: 'MongoDB no está conectado' });
    }
    
    const { days = 30 } = req.query;
    const startDate = new Date();
    startDate.setDate(startDate.getDate() - parseInt(days));
    
    // Obtener pueblos únicos
    const pueblos = await mongoDb.collection('events').aggregate([
      { $match: { server_time: { $gte: startDate } } },
      { $project: { pueblo: { $ifNull: ['$data.pueblo_nombre', '$data.pueblo_id'] } } },
      { $match: { pueblo: { $ne: null, $ne: '' } } },
      { $group: { _id: '$pueblo', count: { $sum: 1 } } },
      { $sort: { count: -1 } }
    ]).toArray();
    
    // Obtener categorías únicas
    const categorias = await mongoDb.collection('events').aggregate([
      { $match: { server_time: { $gte: startDate } } },
      { $project: { categoria: { $ifNull: ['$data.category_name', '$data.category_id'] } } },
      { $match: { categoria: { $ne: null, $ne: '' } } },
      { $group: { _id: '$categoria', count: { $sum: 1 } } },
      { $sort: { count: -1 } }
    ]).toArray();
    
    // Obtener entidades únicas con su información
    const entidades = await mongoDb.collection('events').aggregate([
      { $match: { server_time: { $gte: startDate }, event_name: { $in: ['entity_clicked', 'entity_detail_view'] } } },
      { $project: { 
        entidad: '$data.entity_name', 
        categoria: { $ifNull: ['$data.category_name', '$data.category_id'] },
        pueblo: { $ifNull: ['$data.pueblo_nombre', '$data.pueblo_id'] }
      }},
      { $match: { entidad: { $ne: null, $ne: '' } } },
      { $group: { _id: { entidad: '$entidad', categoria: '$categoria', pueblo: '$pueblo' }, count: { $sum: 1 } } },
      { $sort: { count: -1 } },
      { $limit: 100 }
    ]).toArray();
    
    // Formatear entidades
    const entidadesFormateadas = entidades.map(e => ({
      nombre: e._id.entidad,
      categoria: e._id.categoria,
      pueblo: e._id.pueblo,
      eventos: e.count
    }));
    
    res.json({
      success: true,
      data: {
        pueblos: pueblos.map(p => ({ nombre: p._id, eventos: p.count })),
        categorias: categorias.map(c => ({ nombre: c._id, eventos: c.count })),
        entidades: entidadesFormateadas
      }
    });
    
  } catch (error) {
    console.error('Error obteniendo datos dinámicos:', error);
    res.json({ success: false, error: error.message });
  }
});

/**
 * Enviar notificación
 * POST /api/notifications/send
 */
app.post('/api/notifications/send', async (req, res) => {
  try {
    if (!admin.apps.length) {
      return res.json({ success: false, error: 'Firebase Admin no está inicializado' });
    }
    
    if (!mongoDb) {
      return res.json({ success: false, error: 'MongoDB no está conectado' });
    }
    
    const { 
      title, 
      body, 
      image,
      target_type,  // 'all', 'pueblo', 'categoria', 'tokens'
      target_value, // pueblo_nombre, categoria_nombre, o array de tokens
      action_type,  // 'open_home', 'open_entity', 'open_pueblo', 'open_category', 'open_url'
      action_data   // { entity_id, entity_name, ... } o { url }
    } = req.body;
    
    if (!title || !body) {
      return res.json({ success: false, error: 'title y body son requeridos' });
    }
    
    // Obtener tokens según el target
    let tokens = [];
    
    if (target_type === 'all') {
      const devices = await mongoDb.collection('device_tokens').find({}).toArray();
      tokens = devices.map(d => d.token);
    } else if (target_type === 'pueblo') {
      const devices = await mongoDb.collection('device_tokens').find({ 
        $or: [
          { pueblo_preferido: target_value },
          { pueblo_preferido: target_value?.toLowerCase() },
          { pueblo_preferido: target_value?.toUpperCase() }
        ]
      }).toArray();
      tokens = devices.map(d => d.token);
    } else if (target_type === 'categoria') {
      const devices = await mongoDb.collection('device_tokens').find({ 
        categorias_interes: { $in: [target_value, target_value?.toLowerCase()] }
      }).toArray();
      tokens = devices.map(d => d.token);
    } else if (target_type === 'tokens' && Array.isArray(target_value)) {
      tokens = target_value;
    }
    
    if (tokens.length === 0) {
      return res.json({ success: false, error: 'No hay dispositivos registrados para enviar' });
    }
    
    // Construir mensaje
    const message = {
      notification: {
        title,
        body,
        ...(image && { image })
      },
      data: {
        action: action_type || 'open_home',
        click_action: 'FLUTTER_NOTIFICATION_CLICK',
        ...action_data
      },
      tokens: tokens
    };
    
    // Enviar notificación
    const response = await admin.messaging().sendEachForMulticast(message);
    
    // Guardar en historial
    const historyRecord = {
      titulo: title,
      mensaje: body,
      imagen: image || null,
      target_tipo: target_type,
      target_valor: target_value || null,
      action_tipo: action_type || 'open_home',
      action_datos: action_data || {},
      tokens_enviados: tokens.length,
      enviados_exitosos: response.successCount,
      enviados_fallidos: response.failureCount,
      fecha_envio: new Date(),
      responses: response.responses
    };
    
    await mongoDb.collection('notifications_history').insertOne(historyRecord);
    
    // Limpiar tokens inválidos
    if (response.failureCount > 0) {
      const invalidTokens = [];
      response.responses.forEach((resp, idx) => {
        if (!resp.success && resp.error?.code === 'messaging/invalid-registration-token') {
          invalidTokens.push(tokens[idx]);
        }
      });
      
      if (invalidTokens.length > 0) {
        await mongoDb.collection('device_tokens').deleteMany({ token: { $in: invalidTokens } });
        console.log(`🧹 Eliminados ${invalidTokens.length} tokens inválidos`);
      }
    }
    
    res.json({
      success: true,
      message: 'Notificación enviada',
      stats: {
        total: tokens.length,
        exitosos: response.successCount,
        fallidos: response.failureCount
      }
    });
    
  } catch (error) {
    console.error('Error enviando notificación:', error);
    res.json({ success: false, error: error.message });
  }
});

/**
 * Obtener historial de notificaciones
 * GET /api/notifications/history
 */
app.get('/api/notifications/history', async (req, res) => {
  try {
    if (!mongoDb) {
      return res.json({ success: false, error: 'MongoDB no está conectado' });
    }
    
    const { limit = 20 } = req.query;
    
    const history = await mongoDb.collection('notifications_history')
      .find({})
      .sort({ fecha_envio: -1 })
      .limit(parseInt(limit))
      .toArray();
    
    res.json({ success: true, data: history });
    
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

/**
 * Obtener estadísticas de notificaciones
 * GET /api/notifications/stats
 */
app.get('/api/notifications/stats', async (req, res) => {
  try {
    if (!mongoDb) {
      return res.json({ success: false, error: 'MongoDB no está conectado' });
    }
    
    const [totalDevices, totalNotifications, recentNotifications] = await Promise.all([
      mongoDb.collection('device_tokens').countDocuments(),
      mongoDb.collection('notifications_history').countDocuments(),
      mongoDb.collection('notifications_history').aggregate([
        { $match: { fecha_envio: { $gte: new Date(Date.now() - 30 * 24 * 60 * 60 * 1000) } } },
        { $group: { _id: null, totalEnviados: { $sum: '$tokens_enviados' }, totalExitosos: { $sum: '$enviados_exitosos' } } }
      ]).toArray()
    ]);
    
    const stats = recentNotifications[0] || { totalEnviados: 0, totalExitosos: 0 };
    
    res.json({
      success: true,
      data: {
        dispositivosRegistrados: totalDevices,
        notificacionesEnviadas: totalNotifications,
        totalEnviados30Dias: stats.totalEnviados,
        totalExitosos30Dias: stats.totalExitosos
      }
    });
    
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

/**
 * Eliminar token de dispositivo
 * DELETE /api/notifications/token/:token
 */
app.delete('/api/notifications/token/:token', async (req, res) => {
  try {
    if (!mongoDb) {
      return res.json({ success: false, error: 'MongoDB no está conectado' });
    }
    
    const { token } = req.params;
    
    await mongoDb.collection('device_tokens').deleteOne({ token });
    
    res.json({ success: true, message: 'Token eliminado' });
    
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// ========================================
// NUEVO: PANEL DE ADMINISTRACIÓN HTML
// ========================================

app.get('/admin', (req, res) => {
  res.send(`
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Panel de Notificaciones - Turisteando</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: 'Inter', sans-serif; background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%); min-height: 100vh; color: #fff; }
        .container { max-width: 900px; margin: 0 auto; padding: 20px; }
        
        .header { text-align: center; padding: 30px 0; }
        .header h1 { font-size: 28px; margin-bottom: 8px; }
        .header p { color: #8892b0; }
        
        .card { background: rgba(255,255,255,0.05); border-radius: 16px; padding: 24px; margin-bottom: 20px; border: 1px solid rgba(255,255,255,0.1); }
        .card h2 { font-size: 18px; margin-bottom: 20px; color: #64ffda; display: flex; align-items: center; gap: 10px; }
        
        .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 15px; margin-bottom: 20px; }
        .stat-box { background: rgba(100,255,218,0.1); border-radius: 12px; padding: 20px; text-align: center; }
        .stat-box .number { font-size: 32px; font-weight: 700; color: #64ffda; }
        .stat-box .label { font-size: 12px; color: #8892b0; margin-top: 5px; }
        
        .form-group { margin-bottom: 20px; }
        .form-group label { display: block; margin-bottom: 8px; font-size: 14px; color: #ccd6f6; }
        .form-group input, .form-group textarea, .form-group select { 
            width: 100%; padding: 12px 16px; border-radius: 8px; border: 1px solid rgba(255,255,255,0.2); 
            background: rgba(255,255,255,0.05); color: #fff; font-size: 14px; font-family: inherit;
        }
        .form-group input:focus, .form-group textarea:focus, .form-group select:focus { 
            outline: none; border-color: #64ffda; 
        }
        .form-group textarea { min-height: 80px; resize: vertical; }
        
        .radio-group { display: flex; flex-wrap: wrap; gap: 15px; }
        .radio-item { display: flex; align-items: center; gap: 8px; cursor: pointer; }
        .radio-item input { width: 18px; height: 18px; }
        
        .select-group { margin-top: 15px; padding: 15px; background: rgba(0,0,0,0.2); border-radius: 8px; }
        .select-group.hidden { display: none; }
        
        .btn { padding: 14px 28px; border-radius: 8px; font-size: 14px; font-weight: 600; cursor: pointer; border: none; transition: all 0.3s; }
        .btn-primary { background: #64ffda; color: #1a1a2e; }
        .btn-primary:hover { background: #4cd9b4; transform: translateY(-2px); }
        .btn-secondary { background: rgba(255,255,255,0.1); color: #fff; }
        .btn-secondary:hover { background: rgba(255,255,255,0.2); }
        .btn:disabled { opacity: 0.5; cursor: not-allowed; }
        
        .actions { display: flex; gap: 15px; justify-content: flex-end; margin-top: 20px; }
        
        .history-table { width: 100%; border-collapse: collapse; }
        .history-table th, .history-table td { padding: 12px; text-align: left; border-bottom: 1px solid rgba(255,255,255,0.1); }
        .history-table th { color: #64ffda; font-size: 12px; text-transform: uppercase; }
        .history-table td { font-size: 14px; }
        .history-table tr:hover { background: rgba(255,255,255,0.03); }
        
        .badge { padding: 4px 10px; border-radius: 20px; font-size: 11px; font-weight: 500; }
        .badge-success { background: rgba(100,255,218,0.2); color: #64ffda; }
        .badge-info { background: rgba(100,149,237,0.2); color: #6495ed; }
        
        .loading { text-align: center; padding: 40px; color: #8892b0; }
        .toast { position: fixed; bottom: 20px; right: 20px; padding: 16px 24px; border-radius: 8px; font-size: 14px; z-index: 1000; animation: slideIn 0.3s; }
        .toast-success { background: #64ffda; color: #1a1a2e; }
        .toast-error { background: #ff6b6b; color: #fff; }
        @keyframes slideIn { from { transform: translateX(100%); opacity: 0; } to { transform: translateX(0); opacity: 1; } }
        
        .tab-buttons { display: flex; gap: 10px; margin-bottom: 20px; }
        .tab-btn { padding: 10px 20px; border-radius: 8px; background: rgba(255,255,255,0.05); color: #8892b0; border: none; cursor: pointer; font-size: 14px; }
        .tab-btn.active { background: #64ffda; color: #1a1a2e; }
        
        @media (max-width: 600px) {
            .container { padding: 15px; }
            .header h1 { font-size: 22px; }
            .stats-grid { grid-template-columns: repeat(2, 1fr); }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🔔 Panel de Notificaciones</h1>
            <p>Gestiona las notificaciones push de TuristeandoAPP</p>
        </div>
        
        <!-- Estadísticas -->
        <div class="card">
            <h2>📊 Estadísticas</h2>
            <div class="stats-grid">
                <div class="stat-box">
                    <div class="number" id="stat-devices">-</div>
                    <div class="label">Dispositivos</div>
                </div>
                <div class="stat-box">
                    <div class="number" id="stat-sent">-</div>
                    <div class="label">Enviadas</div>
                </div>
                <div class="stat-box">
                    <div class="number" id="stat-monthly">-</div>
                    <div class="label">Este mes</div>
                </div>
            </div>
        </div>
        
        <!-- Tabs -->
        <div class="tab-buttons">
            <button class="tab-btn active" onclick="showTab('send')">📤 Enviar</button>
            <button class="tab-btn" onclick="showTab('history')">📋 Historial</button>
        </div>
        
        <!-- Formulario de envío -->
        <div class="card" id="tab-send">
            <h2>📝 Crear Notificación</h2>
            
            <div class="form-group">
                <label>Título *</label>
                <input type="text" id="title" placeholder="Ej: Nuevo evento en Villa de Leyva">
            </div>
            
            <div class="form-group">
                <label>Mensaje *</label>
                <textarea id="body" placeholder="Ej: No te pierdas el festival gastronómico este fin de semana"></textarea>
            </div>
            
            <div class="form-group">
                <label>Imagen URL (opcional)</label>
                <input type="text" id="image" placeholder="https://ejemplo.com/imagen.png">
            </div>
            
            <div class="form-group">
                <label>Enviar a</label>
                <div class="radio-group">
                    <label class="radio-item">
                        <input type="radio" name="target" value="all" checked onchange="updateTargetFields()">
                        Todos los usuarios
                    </label>
                    <label class="radio-item">
                        <input type="radio" name="target" value="pueblo" onchange="updateTargetFields()">
                        Por pueblo
                    </label>
                    <label class="radio-item">
                        <input type="radio" name="target" value="categoria" onchange="updateTargetFields()">
                        Por categoría
                    </label>
                </div>
            </div>
            
            <div id="target-pueblo" class="select-group hidden">
                <label>Pueblo</label>
                <select id="select-pueblo"><option value="">Cargando pueblos...</option></select>
            </div>
            
            <div id="target-categoria" class="select-group hidden">
                <label>Categoría</label>
                <select id="select-categoria"><option value="">Cargando categorías...</option></select>
            </div>
            
            <div class="form-group">
                <label>Al tocar la notificación</label>
                <div class="radio-group">
                    <label class="radio-item">
                        <input type="radio" name="action" value="open_home" checked onchange="updateActionFields()">
                        Pantalla principal
                    </label>
                    <label class="radio-item">
                        <input type="radio" name="action" value="open_entity" onchange="updateActionFields()">
                        Entidad específica
                    </label>
                    <label class="radio-item">
                        <input type="radio" name="action" value="open_pueblo" onchange="updateActionFields()">
                        Un pueblo
                    </label>
                    <label class="radio-item">
                        <input type="radio" name="action" value="open_url" onchange="updateActionFields()">
                        URL externa
                    </label>
                </div>
            </div>
            
            <div id="action-entity" class="select-group hidden">
                <label>Entidad</label>
                <select id="select-entity"><option value="">Cargando entidades...</option></select>
            </div>
            
            <div id="action-pueblo" class="select-group hidden">
                <label>Pueblo</label>
                <select id="select-action-pueblo"><option value="">Cargando pueblos...</option></select>
            </div>
            
            <div id="action-url" class="select-group hidden">
                <label>URL</label>
                <input type="text" id="action-url-input" placeholder="https://ejemplo.com/pagina">
            </div>
            
            <div class="actions">
                <button class="btn btn-secondary" onclick="clearForm()">Limpiar</button>
                <button class="btn btn-primary" id="btn-send" onclick="sendNotification()">📤 Enviar Notificación</button>
            </div>
        </div>
        
        <!-- Historial -->
        <div class="card hidden" id="tab-history">
            <h2>📋 Historial de Notificaciones</h2>
            <table class="history-table">
                <thead>
                    <tr>
                        <th>Fecha</th>
                        <th>Título</th>
                        <th>Destino</th>
                        <th>Enviados</th>
                        <th>Estado</th>
                    </tr>
                </thead>
                <tbody id="history-body">
                    <tr><td colspan="5" class="loading">Cargando historial...</td></tr>
                </tbody>
            </table>
        </div>
    </div>
    
    <script>
        // Variables globales
        let dynamicData = { pueblos: [], categorias: [], entidades: [] };
        const API_BASE = window.location.origin;
        
        // Cargar datos al iniciar
        document.addEventListener('DOMContentLoaded', () => {
            loadStats();
            loadDynamicData();
            loadHistory();
        });
        
        // Cargar estadísticas
        async function loadStats() {
            try {
                const res = await fetch(API_BASE + '/api/notifications/stats');
                const data = await res.json();
                if (data.success) {
                    document.getElementById('stat-devices').textContent = data.data.dispositivosRegistrados;
                    document.getElementById('stat-sent').textContent = data.data.notificacionesEnviadas;
                    document.getElementById('stat-monthly').textContent = data.data.totalExitosos30Dias;
                }
            } catch (e) {
                console.error('Error cargando stats:', e);
            }
        }
        
        // Cargar datos dinámicos (pueblos, categorías, entidades)
        async function loadDynamicData() {
            try {
                const res = await fetch(API_BASE + '/api/notifications/dynamic-data?days=30');
                const data = await res.json();
                if (data.success) {
                    dynamicData = data.data;
                    
                    // Poblar select de pueblos
                    const puebloSelect = document.getElementById('select-pueblo');
                    puebloSelect.innerHTML = '<option value="">Selecciona un pueblo</option>' +
                        dynamicData.pueblos.map(p => '<option value="' + p.nombre + '">' + p.nombre + '</option>').join('');
                    
                    // Poblar select de categorías
                    const categoriaSelect = document.getElementById('select-categoria');
                    categoriaSelect.innerHTML = '<option value="">Selecciona una categoría</option>' +
                        dynamicData.categorias.map(c => '<option value="' + c.nombre + '">' + c.nombre + '</option>').join('');
                    
                    // Poblar select de entidades
                    const entitySelect = document.getElementById('select-entity');
                    entitySelect.innerHTML = '<option value="">Selecciona una entidad</option>' +
                        dynamicData.entidades.map(e => '<option value="' + e.nombre + '" data-categoria="' + e.categoria + '" data-pueblo="' + e.pueblo + '">' + e.nombre + '</option>').join('');
                    
                    // Poblar select de pueblos para acción
                    const actionPuebloSelect = document.getElementById('select-action-pueblo');
                    actionPuebloSelect.innerHTML = '<option value="">Selecciona un pueblo</option>' +
                        dynamicData.pueblos.map(p => '<option value="' + p.nombre + '">' + p.nombre + '</option>').join('');
                }
            } catch (e) {
                console.error('Error cargando datos dinámicos:', e);
            }
        }
        
        // Cargar historial
        async function loadHistory() {
            try {
                const res = await fetch(API_BASE + '/api/notifications/history?limit=20');
                const data = await res.json();
                const tbody = document.getElementById('history-body');
                
                if (data.success && data.data.length > 0) {
                    tbody.innerHTML = data.data.map(n => {
                        const fecha = new Date(n.fecha_envio).toLocaleDateString('es-ES');
                        const destino = n.target_tipo === 'all' ? 'Todos' : (n.target_valor || n.target_tipo);
                        return '<tr>' +
                            '<td>' + fecha + '</td>' +
                            '<td>' + n.titulo.substring(0, 30) + (n.titulo.length > 30 ? '...' : '') + '</td>' +
                            '<td>' + destino + '</td>' +
                            '<td>' + n.enviados_exitosos + '/' + n.tokens_enviados + '</td>' +
                            '<td><span class="badge badge-success">Enviado</span></td>' +
                        '</tr>';
                    }).join('');
                } else {
                    tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;color:#8892b0;">No hay notificaciones enviadas</td></tr>';
                }
            } catch (e) {
                console.error('Error cargando historial:', e);
            }
        }
        
        // Mostrar/ocultar campos según selección
        function updateTargetFields() {
            const target = document.querySelector('input[name="target"]:checked').value;
            document.getElementById('target-pueblo').classList.toggle('hidden', target !== 'pueblo');
            document.getElementById('target-categoria').classList.toggle('hidden', target !== 'categoria');
        }
        
        function updateActionFields() {
            const action = document.querySelector('input[name="action"]:checked').value;
            document.getElementById('action-entity').classList.toggle('hidden', action !== 'open_entity');
            document.getElementById('action-pueblo').classList.toggle('hidden', action !== 'open_pueblo');
            document.getElementById('action-url').classList.toggle('hidden', action !== 'open_url');
        }
        
        // Cambiar tabs
        function showTab(tab) {
            document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
            event.target.classList.add('active');
            document.getElementById('tab-send').classList.toggle('hidden', tab !== 'send');
            document.getElementById('tab-history').classList.toggle('hidden', tab !== 'history');
            if (tab === 'history') loadHistory();
        }
        
        // Enviar notificación
        async function sendNotification() {
            const title = document.getElementById('title').value.trim();
            const body = document.getElementById('body').value.trim();
            const image = document.getElementById('image').value.trim();
            const targetType = document.querySelector('input[name="target"]:checked').value;
            const actionType = document.querySelector('input[name="action"]:checked').value;
            
            if (!title || !body) {
                showToast('Por favor completa el título y mensaje', 'error');
                return;
            }
            
            let targetValue = null;
            if (targetType === 'pueblo') {
                targetValue = document.getElementById('select-pueblo').value;
                if (!targetValue) { showToast('Selecciona un pueblo', 'error'); return; }
            } else if (targetType === 'categoria') {
                targetValue = document.getElementById('select-categoria').value;
                if (!targetValue) { showToast('Selecciona una categoría', 'error'); return; }
            }
            
            let actionData = {};
            if (actionType === 'open_entity') {
                const entitySelect = document.getElementById('select-entity');
                const selectedOption = entitySelect.options[entitySelect.selectedIndex];
                if (!entitySelect.value) { showToast('Selecciona una entidad', 'error'); return; }
                actionData = {
                    entity_name: entitySelect.value,
                    category: selectedOption.dataset.categoria || '',
                    pueblo_id: selectedOption.dataset.pueblo || ''
                };
            } else if (actionType === 'open_pueblo') {
                const puebloSelect = document.getElementById('select-action-pueblo');
                if (!puebloSelect.value) { showToast('Selecciona un pueblo', 'error'); return; }
                actionData = { pueblo_id: puebloSelect.value };
            } else if (actionType === 'open_url') {
                const url = document.getElementById('action-url-input').value.trim();
                if (!url) { showToast('Ingresa una URL', 'error'); return; }
                actionData = { url };
            }
            
            const btn = document.getElementById('btn-send');
            btn.disabled = true;
            btn.textContent = '⏳ Enviando...';
            
            try {
                const res = await fetch(API_BASE + '/api/notifications/send', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        title, body, image: image || null,
                        target_type: targetType,
                        target_value: targetValue,
                        action_type: actionType,
                        action_data: actionData
                    })
                });
                
                const data = await res.json();
                
                if (data.success) {
                    showToast('✅ Notificación enviada a ' + data.stats.exitosos + ' dispositivos', 'success');
                    clearForm();
                    loadStats();
                    loadHistory();
                } else {
                    showToast('Error: ' + data.error, 'error');
                }
            } catch (e) {
                showToast('Error de conexión', 'error');
            }
            
            btn.disabled = false;
            btn.textContent = '📤 Enviar Notificación';
        }
        
        // Limpiar formulario
        function clearForm() {
            document.getElementById('title').value = '';
            document.getElementById('body').value = '';
            document.getElementById('image').value = '';
            document.querySelector('input[name="target"][value="all"]').checked = true;
            document.querySelector('input[name="action"][value="open_home"]').checked = true;
            updateTargetFields();
            updateActionFields();
        }
        
        // Mostrar toast
        function showToast(message, type) {
            const toast = document.createElement('div');
            toast.className = 'toast toast-' + type;
            toast.textContent = message;
            document.body.appendChild(toast);
            setTimeout(() => toast.remove(), 4000);
        }
    </script>
</body>
</html>
  `);
});

// ========================================
// INICIAR SERVIDOR
// ========================================

app.listen(PORT, () => {
  console.log(`🚀 Turisteando Analytics Server v4.0 (Notifications) running on port ${PORT}`);
  console.log(`📊 Firebase Property ID: ${PROPERTY_ID}`);
  console.log(`🍃 MongoDB: ${mongoDb ? 'Conectado' : 'No configurado'}`);
  console.log(`🔔 Firebase Admin: ${admin.apps.length > 0 ? 'Inicializado' : 'No configurado'}`);
  console.log(`🖥️ Panel Admin: http://localhost:${PORT}/admin`);
});
