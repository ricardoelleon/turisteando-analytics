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
// FIREBASE ANALYTICS (EXISTENTE - NO SE TOCA)
// ========================================

const analyticsDataClient = new BetaAnalyticsDataClient({
  credentials: {
    client_email: process.env.SERVICE_ACCOUNT_EMAIL,
    private_key: process.env.SERVICE_ACCOUNT_PRIVATE_KEY.replace(/\\n/g, '\n'),
  },
});

const PROPERTY_ID = process.env.PROPERTY_ID || '487082948';

// ========================================
// MONGODB ATLAS (DINÁMICO - ANALYTICS PERSONALIZADO)
// ========================================

const MONGODB_URI = process.env.MONGODB_URI;
const DB_NAME = 'turisteando_analytics';
const COLLECTION_NAME = 'events';

let mongoClient;
let db;

async function connectMongoDB() {
  try {
    if (!MONGODB_URI) {
      console.log('⚠️ MONGODB_URI no configurado. MongoDB deshabilitado.');
      return;
    }
    
    mongoClient = new MongoClient(MONGODB_URI);
    await mongoClient.connect();
    db = mongoClient.db(DB_NAME);
    console.log('✅ Conectado a MongoDB Atlas');
    console.log(`📊 Base de datos: ${DB_NAME}`);
    console.log(`📁 Colección: ${COLLECTION_NAME}`);
    
    // Crear índices para mejor rendimiento
    await db.collection(COLLECTION_NAME).createIndex({ eventName: 1 });
    await db.collection(COLLECTION_NAME).createIndex({ timestamp: -1 });
    await db.collection(COLLECTION_NAME).createIndex({ user_id: 1 });
    console.log('📊 Índices creados');
    
  } catch (error) {
    console.error('❌ Error conectando a MongoDB:', error.message);
    console.log('⚠️ El servidor continuará sin MongoDB (solo Firebase Analytics)');
  }
}

connectMongoDB();

// ========================================
// HELPER FUNCTIONS - FIREBASE ANALYTICS
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
// MIDDLEWARE PARA LOGGING
// ========================================

app.use((req, res, next) => {
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.path}`);
  next();
});

// ========================================
// ENDPOINTS MONGODB - 100% DINÁMICOS
// ========================================

// Guardar evento de analytics desde la app - COMPLETAMENTE DINÁMICO
app.post('/api/mongodb/event', async (req, res) => {
  try {
    if (!db) {
      return res.json({ success: false, error: 'MongoDB no conectado' });
    }

    // Aceptar CUALQUIER campo que venga del cliente
    const event = {
      ...req.body,
      timestamp: new Date(),
      serverTime: new Date().toISOString(),
    };

    const result = await db.collection(COLLECTION_NAME).insertOne(event);
    
    console.log(`📝 Evento guardado: ${event.eventName} - ID: ${result.insertedId}`);
    
    res.json({ 
      success: true, 
      insertedId: result.insertedId,
      message: 'Evento guardado correctamente',
      fieldsReceived: Object.keys(req.body)
    });
  } catch (error) {
    console.error('Error guardando evento:', error);
    res.json({ success: false, error: error.message });
  }
});

// Guardar múltiples eventos (batch) - DINÁMICO
app.post('/api/mongodb/events/batch', async (req, res) => {
  try {
    if (!db) {
      return res.json({ success: false, error: 'MongoDB no conectado' });
    }

    const events = req.body.events.map(event => ({
      ...event,
      timestamp: new Date(),
      serverTime: new Date().toISOString(),
    }));

    const result = await db.collection(COLLECTION_NAME).insertMany(events);
    
    res.json({ 
      success: true, 
      insertedCount: result.insertedCount,
      message: `${result.insertedCount} eventos guardados`
    });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// ========================================
// DESCUBRIMIENTO AUTOMÁTICO DE CAMPOS
// ========================================

// Descubrir todos los campos disponibles - DINÁMICO
app.get('/api/mongodb/campos-disponibles', async (req, res) => {
  try {
    if (!db) {
      return res.json({ success: false, error: 'MongoDB no conectado' });
    }

    // Obtener todos los campos únicos de todos los documentos
    const allKeys = await db.collection(COLLECTION_NAME).aggregate([
      { $project: { arrayofkeyvalue: { $objectToArray: '$$ROOT' } } },
      { $unwind: '$arrayofkeyvalue' },
      { $group: { 
          _id: null, 
          allkeys: { $addToSet: '$arrayofkeyvalue.k' } 
        } 
      }
    ]).toArray();

    const campos = allKeys[0]?.allkeys || [];
    
    // Excluir campos del sistema
    const camposSistema = ['_id', 'timestamp', 'serverTime'];
    const camposUsuario = campos.filter(c => !camposSistema.includes(c)).sort();

    // Obtener valores únicos para cada campo
    const camposConValores = {};
    for (const campo of camposUsuario) {
      const valores = await db.collection(COLLECTION_NAME).distinct(campo);
      camposConValores[campo] = valores.slice(0, 50); // Máximo 50 valores
    }

    res.json({
      success: true,
      data: {
        campos: camposUsuario,
        camposConValores,
        totalCampos: camposUsuario.length
      }
    });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// Descubrir todos los tipos de eventos - DINÁMICO
app.get('/api/mongodb/tipos-eventos', async (req, res) => {
  try {
    if (!db) {
      return res.json({ success: false, error: 'MongoDB no conectado' });
    }

    const eventos = await db.collection(COLLECTION_NAME).aggregate([
      { $group: { 
          _id: '$eventName', 
          count: { $sum: 1 },
          ultimoRegistro: { $max: '$timestamp' }
        } 
      },
      { $sort: { count: -1 } }
    ]).toArray();

    res.json({
      success: true,
      data: eventos.map(e => ({
        eventName: e._id,
        count: e.count,
        ultimoRegistro: e.ultimoRegistro
      }))
    });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// ========================================
// CONSULTA DINÁMICA - CUALQUIER CAMPO
// ========================================

// Obtener eventos con filtros dinámicos
app.get('/api/mongodb/events', async (req, res) => {
  try {
    if (!db) {
      return res.json({ success: false, error: 'MongoDB no conectado' });
    }

    const {
      limit = 100,
      page = 1,
      ...filtros  // CUALQUIER filtro pasado como query param
    } = req.query;

    // Construir filtro dinámicamente
    const filter = {};
    
    Object.keys(filtros).forEach(key => {
      if (filtros[key]) {
        // Soportar múltiples valores separados por coma
        if (filtros[key].includes(',')) {
          filter[key] = { $in: filtros[key].split(',') };
        } else {
          // Búsqueda parcial para strings
          if (isNaN(filtros[key])) {
            filter[key] = { $regex: filtros[key], $options: 'i' };
          } else {
            filter[key] = filtros[key];
          }
        }
      }
    });

    const skip = (parseInt(page) - 1) * parseInt(limit);
    
    const events = await db.collection(COLLECTION_NAME)
      .find(filter)
      .sort({ timestamp: -1 })
      .skip(skip)
      .limit(parseInt(limit))
      .toArray();

    const total = await db.collection(COLLECTION_NAME).countDocuments(filter);

    res.json({
      success: true,
      data: events,
      pagination: {
        total,
        page: parseInt(page),
        limit: parseInt(limit),
        pages: Math.ceil(total / parseInt(limit))
      },
      filtrosAplicados: filter
    });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// ========================================
// AGREGACIÓN DINÁMICA - PARA DASHBOARD
// ========================================

// Agregación genérica por cualquier campo
app.get('/api/mongodb/agregar/:campo', async (req, res) => {
  try {
    if (!db) {
      return res.json({ success: false, error: 'MongoDB no conectado' });
    }

    const { campo } = req.params;
    const { 
      startDate, 
      endDate, 
      eventName,
      filtro,        // JSON string con filtros adicionales
      limit = 20,
      sortBy = 'count',
      sortOrder = 'desc'
    } = req.query;

    // Construir match stage
    const matchStage = {};
    
    if (eventName) matchStage.eventName = eventName;
    
    if (startDate || endDate) {
      matchStage.timestamp = {};
      if (startDate) matchStage.timestamp.$gte = new Date(startDate);
      if (endDate) matchStage.timestamp.$lte = new Date(endDate);
    }
    
    // Agregar filtros adicionales
    if (filtro) {
      try {
        const filtrosAdicionales = JSON.parse(filtro);
        Object.assign(matchStage, filtrosAdicionales);
      } catch (e) {
        // Ignorar si no es JSON válido
      }
    }

    const sortDirection = sortOrder === 'desc' ? -1 : 1;
    const sortField = sortBy === 'count' ? 'count' : campo;

    const resultado = await db.collection(COLLECTION_NAME).aggregate([
      { $match: matchStage },
      { $group: { 
          _id: `$${campo}`, 
          count: { $sum: 1 },
          uniqueUsers: { $addToSet: '$user_id' },
          ultimoRegistro: { $max: '$timestamp' }
        } 
      },
      { 
        $project: { 
          [campo]: '$_id', 
          count: 1, 
          uniqueUsers: { $size: '$uniqueUsers' },
          ultimoRegistro: 1,
          _id: 0 
        } 
      },
      { $sort: { [sortField]: sortDirection } },
      { $limit: parseInt(limit) }
    ]).toArray();

    res.json({
      success: true,
      campo,
      totalValores: resultado.length,
      data: resultado
    });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// Agregación por múltiples campos
app.get('/api/mongodb/agregar-multiple', async (req, res) => {
  try {
    if (!db) {
      return res.json({ success: false, error: 'MongoDB no conectado' });
    }

    const { 
      campos,        // Separados por coma: pueblo_nombre,category_name
      startDate, 
      endDate, 
      eventName,
      limit = 30
    } = req.query;

    if (!campos) {
      return res.json({ 
        success: false, 
        error: 'campos es requerido',
        ejemplo: '/api/mongodb/agregar-multiple?campos=pueblo_nombre,category_name'
      });
    }

    const camposList = campos.split(',').map(c => c.trim());

    // Construir match stage
    const matchStage = {};
    
    if (eventName) matchStage.eventName = eventName;
    
    if (startDate || endDate) {
      matchStage.timestamp = {};
      if (startDate) matchStage.timestamp.$gte = new Date(startDate);
      if (endDate) matchStage.timestamp.$lte = new Date(endDate);
    }

    // Construir $group dinámicamente
    const groupId = {};
    camposList.forEach(campo => {
      groupId[campo] = `$${campo}`;
    });

    const resultado = await db.collection(COLLECTION_NAME).aggregate([
      { $match: matchStage },
      { $group: { 
          _id: groupId, 
          count: { $sum: 1 },
          uniqueUsers: { $addToSet: '$user_id' }
        } 
      },
      { 
        $project: { 
          ...camposList.reduce((acc, campo) => {
            acc[campo] = `$_id.${campo}`;
            return acc;
          }, {}),
          count: 1, 
          uniqueUsers: { $size: '$uniqueUsers' },
          _id: 0 
        } 
      },
      { $sort: { count: -1 } },
      { $limit: parseInt(limit) }
    ]).toArray();

    res.json({
      success: true,
      campos: camposList,
      data: resultado
    });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// ========================================
// DASHBOARD DINÁMICO - AUTO-DESCUBIERTO
// ========================================

// Resumen general - DINÁMICO
app.get('/api/mongodb/overview', async (req, res) => {
  try {
    if (!db) {
      return res.json({ success: false, error: 'MongoDB no conectado' });
    }

    const { startDate, endDate } = req.query;
    
    const matchStage = {};
    if (startDate || endDate) {
      matchStage.timestamp = {};
      if (startDate) matchStage.timestamp.$gte = new Date(startDate);
      if (endDate) matchStage.timestamp.$lte = new Date(endDate);
    }

    const stats = await db.collection(COLLECTION_NAME).aggregate([
      { $match: Object.keys(matchStage).length > 0 ? matchStage : {} },
      {
        $facet: {
          totalEvents: [{ $count: 'count' }],
          uniqueUsers: [
            { $group: { _id: '$user_id' } },
            { $count: 'count' }
          ],
          eventTypes: [
            { $group: { _id: '$eventName', count: { $sum: 1 } } },
            { $sort: { count: -1 } },
            { $limit: 20 }
          ],
          eventsByDate: [
            {
              $group: {
                _id: {
                  $dateToString: { format: '%Y-%m-%d', date: '$timestamp' }
                },
                count: { $sum: 1 }
              }
            },
            { $sort: { _id: 1 } },
            { $limit: 30 }
          ],
          eventsByHour: [
            {
              $group: {
                _id: { $hour: '$timestamp' },
                count: { $sum: 1 }
              }
            },
            { $sort: { _id: 1 } }
          ]
        }
      }
    ]).toArray();

    const result = stats[0];

    res.json({
      success: true,
      data: {
        totalEvents: result.totalEvents[0]?.count || 0,
        uniqueUsers: result.uniqueUsers[0]?.count || 0,
        eventTypes: result.eventTypes.map(e => ({ eventName: e._id, count: e.count })),
        eventsByDate: result.eventsByDate.map(e => ({ date: e._id, count: e.count })),
        eventsByHour: result.eventsByHour.map(e => ({ hour: e._id, count: e.count }))
      }
    });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// Dashboard completo auto-descubierto - DINÁMICO
app.get('/api/mongodb/dashboard', async (req, res) => {
  try {
    if (!db) {
      return res.json({ success: false, error: 'MongoDB no conectado' });
    }

    const { startDate, endDate } = req.query;

    const matchStage = {};
    if (startDate || endDate) {
      matchStage.timestamp = {};
      if (startDate) matchStage.timestamp.$gte = new Date(startDate);
      if (endDate) matchStage.timestamp.$lte = new Date(endDate);
    }

    // Descubrir campos automáticamente
    const camposDescubiertos = await db.collection(COLLECTION_NAME).aggregate([
      { $match: Object.keys(matchStage).length > 0 ? matchStage : {} },
      { $project: { arrayofkeyvalue: { $objectToArray: '$$ROOT' } } },
      { $unwind: '$arrayofkeyvalue' },
      { $group: { _id: '$arrayofkeyvalue.k', count: { $sum: 1 } } },
      { $match: { _id: { $nin: ['_id', 'timestamp', 'serverTime', 'user_id', 'eventName'] } } },
      { $sort: { count: -1 } },
      { $limit: 20 }
    ]).toArray();

    const campos = camposDescubiertos.map(c => c._id);

    // Generar agregaciones para cada campo descubierto
    const agregaciones = {};
    
    for (const campo of campos) {
      const datos = await db.collection(COLLECTION_NAME).aggregate([
        { $match: { ...matchStage, [campo]: { $exists: true, $ne: null, $ne: '' } } },
        { $group: { _id: `$${campo}`, count: { $sum: 1 } } },
        { $project: { [campo]: '$_id', count: 1, _id: 0 } },
        { $sort: { count: -1 } },
        { $limit: 15 }
      ]).toArray();
      
      if (datos.length > 0) {
        agregaciones[campo] = datos;
      }
    }

    // Overview
    const overview = await db.collection(COLLECTION_NAME).aggregate([
      { $match: Object.keys(matchStage).length > 0 ? matchStage : {} },
      {
        $facet: {
          totalEvents: [{ $count: 'count' }],
          uniqueUsers: [{ $group: { _id: '$user_id' } }, { $count: 'count' }],
          eventTypes: [{ $group: { _id: '$eventName', count: { $sum: 1 } } }, { $sort: { count: -1 } }, { $limit: 10 }]
        }
      }
    ]).toArray();

    // Eventos por día
    const eventosPorDia = await db.collection(COLLECTION_NAME).aggregate([
      { $match: Object.keys(matchStage).length > 0 ? matchStage : {} },
      { $group: { _id: { $dateToString: { format: '%Y-%m-%d', date: '$timestamp' } }, count: { $sum: 1 } } },
      { $project: { date: '$_id', count: 1, _id: 0 } },
      { $sort: { date: 1 } },
      { $limit: 30 }
    ]).toArray();

    res.json({
      success: true,
      data: {
        overview: {
          totalEvents: overview[0]?.totalEvents[0]?.count || 0,
          uniqueUsers: overview[0]?.uniqueUsers[0]?.count || 0,
          eventTypes: overview[0]?.eventTypes || []
        },
        camposDescubiertos: campos,
        agregaciones,
        eventosPorDia,
        generatedAt: new Date().toISOString()
      }
    });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// ========================================
// ENDPOINTS DE CONVENIENCIA (Mantienen compatibilidad)
// ========================================

// Pueblos - usa agregación dinámica
app.get('/api/mongodb/pueblos', async (req, res) => {
  try {
    if (!db) {
      return res.json({ success: false, error: 'MongoDB no conectado' });
    }

    const { startDate, endDate, limit = 20 } = req.query;

    const matchStage = {
      pueblo_nombre: { $exists: true, $ne: null }
    };
    
    if (startDate || endDate) {
      matchStage.timestamp = {};
      if (startDate) matchStage.timestamp.$gte = new Date(startDate);
      if (endDate) matchStage.timestamp.$lte = new Date(endDate);
    }

    const pueblos = await db.collection(COLLECTION_NAME).aggregate([
      { $match: matchStage },
      { $group: {
          _id: '$pueblo_nombre',
          views: { $sum: 1 },
          uniqueUsers: { $addToSet: '$user_id' }
        }
      },
      { $project: { pueblo: '$_id', views: 1, uniqueUsers: { $size: '$uniqueUsers' }, _id: 0 } },
      { $sort: { views: -1 } },
      { $limit: parseInt(limit) }
    ]).toArray();

    res.json({ success: true, data: pueblos });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// Categorías - usa agregación dinámica
app.get('/api/mongodb/categorias', async (req, res) => {
  try {
    if (!db) {
      return res.json({ success: false, error: 'MongoDB no conectado' });
    }

    const { startDate, endDate, pueblo_nombre, limit = 20 } = req.query;

    const matchStage = {
      category_name: { $exists: true, $ne: null }
    };
    
    if (pueblo_nombre) matchStage.pueblo_nombre = pueblo_nombre;
    
    if (startDate || endDate) {
      matchStage.timestamp = {};
      if (startDate) matchStage.timestamp.$gte = new Date(startDate);
      if (endDate) matchStage.timestamp.$lte = new Date(endDate);
    }

    const categorias = await db.collection(COLLECTION_NAME).aggregate([
      { $match: matchStage },
      { $group: {
          _id: { categoria: '$category_name', pueblo: '$pueblo_nombre' },
          clicks: { $sum: 1 },
          uniqueUsers: { $addToSet: '$user_id' }
        }
      },
      { $project: { categoria: '$_id.categoria', pueblo: '$_id.pueblo', clicks: 1, uniqueUsers: { $size: '$uniqueUsers' }, _id: 0 } },
      { $sort: { clicks: -1 } },
      { $limit: parseInt(limit) }
    ]).toArray();

    res.json({ success: true, data: categorias });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// Entidades - usa agregación dinámica
app.get('/api/mongodb/entidades', async (req, res) => {
  try {
    if (!db) {
      return res.json({ success: false, error: 'MongoDB no conectado' });
    }

    const { startDate, endDate, pueblo_nombre, category_name, limit = 30 } = req.query;

    const matchStage = {
      entity_name: { $exists: true, $ne: null }
    };
    
    if (pueblo_nombre) matchStage.pueblo_nombre = pueblo_nombre;
    if (category_name) matchStage.category_name = category_name;
    
    if (startDate || endDate) {
      matchStage.timestamp = {};
      if (startDate) matchStage.timestamp.$gte = new Date(startDate);
      if (endDate) matchStage.timestamp.$lte = new Date(endDate);
    }

    const entidades = await db.collection(COLLECTION_NAME).aggregate([
      { $match: matchStage },
      { $group: {
          _id: { entidad: '$entity_name', categoria: '$category_name', pueblo: '$pueblo_nombre' },
          clicks: { $sum: 1 },
          uniqueUsers: { $addToSet: '$user_id' }
        }
      },
      { $project: { entidad: '$_id.entidad', categoria: '$_id.categoria', pueblo: '$_id.pueblo', clicks: 1, uniqueUsers: { $size: '$uniqueUsers' }, _id: 0 } },
      { $sort: { clicks: -1 } },
      { $limit: parseInt(limit) }
    ]).toArray();

    res.json({ success: true, data: entidades });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// Acciones - usa agregación dinámica
app.get('/api/mongodb/acciones', async (req, res) => {
  try {
    if (!db) {
      return res.json({ success: false, error: 'MongoDB no conectado' });
    }

    const { startDate, endDate, limit = 30 } = req.query;

    const matchStage = {
      action: { $exists: true, $ne: null }
    };
    
    if (startDate || endDate) {
      matchStage.timestamp = {};
      if (startDate) matchStage.timestamp.$gte = new Date(startDate);
      if (endDate) matchStage.timestamp.$lte = new Date(endDate);
    }

    const acciones = await db.collection(COLLECTION_NAME).aggregate([
      { $match: matchStage },
      { $group: { _id: { action: '$action', entidad: '$entity_name' }, count: { $sum: 1 } } },
      { $project: { action: '$_id.action', entidad: '$_id.entidad', count: 1, _id: 0 } },
      { $sort: { count: -1 } },
      { $limit: parseInt(limit) }
    ]).toArray();

    const resumen = await db.collection(COLLECTION_NAME).aggregate([
      { $match: matchStage },
      { $group: { _id: '$action', count: { $sum: 1 } } },
      { $project: { action: '$_id', count: 1, _id: 0 } },
      { $sort: { count: -1 } }
    ]).toArray();

    res.json({ success: true, data: { detalles: acciones, resumen } });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// ========================================
// ENDPOINTS FIREBASE ANALYTICS (EXISTENTES)
// ========================================

app.get('/api/health', (req, res) => {
  res.json({ 
    status: 'OK', 
    timestamp: new Date().toISOString(),
    propertyId: PROPERTY_ID,
    mongodb: db ? 'connected' : 'disconnected',
    version: '4.0.0-mongodb-dynamic'
  });
});

// Overview - KPIs generales (Firebase)
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

// Users by day (Firebase)
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

// Top events (Firebase)
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

// By platform (Firebase)
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

// By country (Firebase)
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

// Top screens (Firebase)
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
// LIMPIEZA DE DATOS (OPCIONAL)
// ========================================

app.delete('/api/mongodb/events/old', async (req, res) => {
  try {
    if (!db) {
      return res.json({ success: false, error: 'MongoDB no conectado' });
    }

    const { days = 90 } = req.query;
    const cutoffDate = new Date();
    cutoffDate.setDate(cutoffDate.getDate() - parseInt(days));

    const result = await db.collection(COLLECTION_NAME).deleteMany({
      timestamp: { $lt: cutoffDate }
    });

    res.json({
      success: true,
      deletedCount: result.deletedCount,
      message: `Eliminados ${result.deletedCount} eventos anteriores a ${cutoffDate.toISOString()}`
    });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// Cerrar conexión MongoDB al terminar
process.on('SIGINT', async () => {
  if (mongoClient) {
    await mongoClient.close();
    console.log('MongoDB connection closed');
  }
  process.exit(0);
});

app.listen(PORT, () => {
  console.log(`🚀 Turisteando Analytics Server v4.0 (MongoDB Dynamic) running on port ${PORT}`);
  console.log(`📊 Firebase Property ID: ${PROPERTY_ID}`);
  console.log(`🍃 MongoDB: ${db ? 'Conectado' : 'No configurado'}`);
  console.log(`🔗 Health check: http://localhost:${PORT}/api/health`);
});
