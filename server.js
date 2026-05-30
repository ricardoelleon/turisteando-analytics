const express = require('express');
const cors = require('cors');
const { BetaAnalyticsDataClient } = require('@google-analytics/data');
const { MongoClient, ObjectId } = require('mongodb');
// ========================================
// NUEVO: Firebase Admin para notificaciones
// ========================================
const admin = require('firebase-admin');
// ========================================
// NUEVO: node-cron para notificaciones recurrentes y programadas
// ========================================
const cron = require('node-cron');

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
// MONGODB ATLAS (tu conexión existente)
// ========================================

const MONGO_URI = process.env.MONGODB_URI;
let mongoClient = null;
let mongoDb = null;

// ========================================
// NUEVO: Colecciones para notificaciones
// ========================================
let deviceTokensCollection;
let notificationsHistoryCollection;
let scheduledNotificationsCollection;
let recurringNotificationsCollection;
let userPreferencesCollection;

// ========================================
// NUEVO: Firestore Database
// ========================================
let firestoreDb;

async function connectMongoDB() {
  if (!MONGO_URI) {
    console.log('⚠️ MONGODB_URI no configurado - MongoDB deshabilitado');
    return false;
  }
  
  try {
    mongoClient = new MongoClient(MONGO_URI);
    await mongoClient.connect();
    mongoDb = mongoClient.db('turisteando_analytics');
    
    // Inicializar colecciones de notificaciones
    deviceTokensCollection = mongoDb.collection('device_tokens');
    notificationsHistoryCollection = mongoDb.collection('notifications_history');
    scheduledNotificationsCollection = mongoDb.collection('scheduled_notifications');
    recurringNotificationsCollection = mongoDb.collection('recurring_notifications');
    userPreferencesCollection = mongoDb.collection('user_preferences');
    
    // Crear índices para mejor rendimiento
    await deviceTokensCollection.createIndex({ token: 1 }, { unique: true }).catch(() => {});
    await notificationsHistoryCollection.createIndex({ fecha_envio: -1 }).catch(() => {});
    await scheduledNotificationsCollection.createIndex({ scheduled_date: 1, sent: 1 }).catch(() => {});
    await recurringNotificationsCollection.createIndex({ active: 1 }).catch(() => {});
    
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
        projectId: 'turisteandoapp-ee561',
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

// ========================================
// NUEVO: Inicializar Firestore
// ========================================
function initializeFirestore() {
  try {
    if (admin.apps.length > 0) {
      firestoreDb = admin.firestore();
      console.log('✅ Firestore inicializado para datos dinámicos');
      return true;
    }
    console.log('⚠️ Firestore no se pudo inicializar - Firebase Admin no está listo');
    return false;
  } catch (error) {
    console.error('❌ Error inicializando Firestore:', error.message);
    return false;
  }
}

// Inicializar después de conectar MongoDB
initializeFirebaseAdmin();
// Inicializar Firestore después de Firebase Admin
initializeFirestore();

// ========================================
// HELPER FUNCTION: Normalizar ID (igual que en Android)
// ========================================
function normalizeId(id) {
  if (!id) return '';
  
  return id.toLowerCase()
    .trim()
    .replace(/á/g, 'a')
    .replace(/é/g, 'e')
    .replace(/í/g, 'i')
    .replace(/ó/g, 'o')
    .replace(/ú/g, 'u')
    .replace(/ü/g, 'u')
    .replace(/ñ/g, 'n')
    .replace(/\s+/g, '_')
    .replace(/[^a-z0-9_]/g, '');
}

// ========================================
// HELPER FUNCTIONS (tu código existente)
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
// FUNCIÓN INTELIGENTE - VOTO MAYORITARIO (tu código existente)
// ========================================

function calculateCorrectCategories(entidadesData) {
  const entidadStats = new Map();
  
  entidadesData.forEach(e => {
    const entidadNombre = e._id.entidad;
    const categoria = e._id.categoria || 'sin_categoria';
    const pueblo = e._id.pueblo;
    const clicks = e.clicks;
    
    if (!entidadNombre) return;
    
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
    
    const currentCount = stats.categorias.get(categoria) || 0;
    stats.categorias.set(categoria, currentCount + clicks);
  });
  
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
// MIDDLEWARE PARA LOGGING (tu código existente)
// ========================================

app.use((req, res, next) => {
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.path}`);
  next();
});

// ========================================
// ========================================
// NUEVO: ENDPOINTS FIRESTORE - DATOS DINÁMICOS
// ========================================
// ========================================

/**
 * Obtener TODOS los pueblos desde Firestore
 */
app.get('/api/firestore/pueblos', async (req, res) => {
  try {
    if (!firestoreDb) {
      return res.json({ success: false, error: 'Firestore no está inicializado' });
    }

    const pueblosSnapshot = await firestoreDb.collection('pueblos').get();
    
    const pueblos = [];
    pueblosSnapshot.forEach(doc => {
      const data = doc.data();
      pueblos.push({
        id: doc.id,
        nombre: data.nombre || doc.id,
        categorias_activas: data.categorias_activas || [],
        imagen: data.imagen || null,
        descripcion: data.descripcion || null
      });
    });

    console.log(`✅ Obtenidos ${pueblos.length} pueblos desde Firestore`);
    
    res.json({
      success: true,
      data: pueblos
    });

  } catch (error) {
    console.error('❌ Error obteniendo pueblos:', error.message);
    res.json({ success: false, error: error.message });
  }
});

/**
 * Obtener TODAS las categorías únicas desde Firestore
 * Lee las categorias_activas de cada pueblo Y explora TODAS las subcolecciones
 */
app.get('/api/firestore/categorias', async (req, res) => {
  try {
    if (!firestoreDb) {
      return res.json({ success: false, error: 'Firestore no está inicializado' });
    }

    const pueblosSnapshot = await firestoreDb.collection('pueblos').get();
    
    // Recopilar todas las categorías únicas de todos los pueblos
    const categoriasSet = new Set();
    
    // Procesar cada pueblo
    for (const doc of pueblosSnapshot.docs) {
      const data = doc.data();
      const puebloId = doc.id;
      const categoriasActivas = data.categorias_activas || [];
      
      // Agregar categorías de categorias_activas
      categoriasActivas.forEach(cat => categoriasSet.add(cat));
      
      // ============================================================
      // NUEVO: También explorar subcolecciones existentes
      // ============================================================
      try {
        const subcollections = await firestoreDb
          .collection('pueblos')
          .doc(puebloId)
          .listCollections();
        
        for (const subcol of subcollections) {
          categoriasSet.add(subcol.id);
          console.log(`  📂 Categoría encontrada como subcolección: ${puebloId}/${subcol.id}`);
        }
      } catch (listError) {
        console.log(`  ⚠️ No se pudieron listar subcolecciones de ${puebloId}`);
      }
    }

    // Convertir a array con formato
    const categorias = Array.from(categoriasSet)
      .sort()
      .map(cat => ({
        id: normalizeId(cat),
        nombre: cat.charAt(0).toUpperCase() + cat.slice(1).replace(/_/g, ' '),
        original: cat
      }));

    console.log(`✅ Obtenidas ${categorias.length} categorías únicas desde Firestore:`, categorias.map(c => c.id));
    
    res.json({
      success: true,
      total: categorias.length,
      data: categorias
    });

  } catch (error) {
    console.error('❌ Error obteniendo categorías:', error.message);
    res.json({ success: false, error: error.message });
  }
});

/**
 * Obtener categorías de un pueblo específico
 */
app.get('/api/firestore/pueblos/:puebloId/categorias', async (req, res) => {
  try {
    if (!firestoreDb) {
      return res.json({ success: false, error: 'Firestore no está inicializado' });
    }

    const { puebloId } = req.params;
    const puebloIdNormalizado = normalizeId(puebloId);
    
    const puebloDoc = await firestoreDb.collection('pueblos').doc(puebloIdNormalizado).get();
    
    if (!puebloDoc.exists) {
      return res.json({ success: false, error: 'Pueblo no encontrado' });
    }

    const data = puebloDoc.data();
    const categoriasActivas = data.categorias_activas || [];
    
    const categorias = categoriasActivas.map(cat => ({
      id: normalizeId(cat),
      nombre: cat.charAt(0).toUpperCase() + cat.slice(1).replace(/_/g, ' ')
    }));

    res.json({
      success: true,
      pueblo: puebloIdNormalizado,
      data: categorias
    });

  } catch (error) {
    console.error('❌ Error obteniendo categorías del pueblo:', error.message);
    res.json({ success: false, error: error.message });
  }
});

/**
 * Obtener entidades de una categoría específica en un pueblo
 */
app.get('/api/firestore/pueblos/:puebloId/categorias/:categoriaId/entidades', async (req, res) => {
  try {
    if (!firestoreDb) {
      return res.json({ success: false, error: 'Firestore no está inicializado' });
    }

    const { puebloId, categoriaId } = req.params;
    
    // Normalizar IDs
    const puebloIdNormalizado = normalizeId(puebloId);
    const categoriaNormalizada = normalizeId(categoriaId);
    
    const entidadesSnapshot = await firestoreDb
      .collection('pueblos')
      .doc(puebloIdNormalizado)
      .collection(categoriaNormalizada)
      .get();
    
    const entidades = [];
    entidadesSnapshot.forEach(doc => {
      const data = doc.data();
      entidades.push({
        id: doc.id,
        nombre: data.nombre || doc.id,
        categoria: categoriaNormalizada,
        pueblo: puebloIdNormalizado,
        imagen: data.imagen || data.imagen_principal || null,
        descripcion: data.descripcion || null
      });
    });

    res.json({
      success: true,
      pueblo: puebloIdNormalizado,
      categoria: categoriaNormalizada,
      data: entidades
    });

  } catch (error) {
    console.error('❌ Error obteniendo entidades:', error.message);
    res.json({ success: false, error: error.message });
  }
});

/**
 * Obtener TODAS las entidades de un pueblo (de todas sus categorías)
 * SIN LÍMITES - carga todas las entidades disponibles
 */
app.get('/api/firestore/pueblos/:puebloId/entidades', async (req, res) => {
  try {
    if (!firestoreDb) {
      return res.json({ success: false, error: 'Firestore no está inicializado' });
    }

    const { puebloId } = req.params;
    const puebloIdNormalizado = normalizeId(puebloId);
    
    // Primero obtener las categorías activas del pueblo
    const puebloDoc = await firestoreDb.collection('pueblos').doc(puebloIdNormalizado).get();
    
    if (!puebloDoc.exists) {
      return res.json({ success: false, error: 'Pueblo no encontrado' });
    }

    const data = puebloDoc.data();
    const categoriasActivas = data.categorias_activas || [];
    
    // También explorar subcolecciones existentes
    let todasLasCategorias = [...categoriasActivas];
    try {
      const subcollections = await firestoreDb
        .collection('pueblos')
        .doc(puebloIdNormalizado)
        .listCollections();
      
      for (const subcol of subcollections) {
        if (!todasLasCategorias.includes(subcol.id)) {
          todasLasCategorias.push(subcol.id);
        }
      }
    } catch (e) {
      console.log(`⚠️ No se pudieron listar subcolecciones: ${e.message}`);
    }
    
    const entidades = [];
    
    // Obtener entidades de cada categoría (SIN límite)
    for (const categoria of todasLasCategorias) {
      const categoriaNormalizada = normalizeId(categoria);
      
      try {
        // SIN .limit() - cargar TODAS las entidades
        const entidadesSnapshot = await firestoreDb
          .collection('pueblos')
          .doc(puebloIdNormalizado)
          .collection(categoriaNormalizada)
          .get();
        
        entidadesSnapshot.forEach(doc => {
          const entityData = doc.data();
          entidades.push({
            id: doc.id,
            nombre: entityData.nombre || doc.id,
            categoria: categoriaNormalizada,
            pueblo: puebloIdNormalizado,
            imagen: entityData.imagen || entityData.imagen_principal || null
          });
        });
        
        if (!entidadesSnapshot.empty) {
          console.log(`  ✅ ${puebloIdNormalizado}/${categoriaNormalizada}: ${entidadesSnapshot.size} entidades`);
        }
      } catch (e) {
        console.log(`⚠️ No se pudo leer categoría ${categoriaNormalizada}:`, e.message);
      }
    }

    console.log(`📊 Total entidades en ${puebloIdNormalizado}: ${entidades.length}`);

    res.json({
      success: true,
      pueblo: puebloIdNormalizado,
      total: entidades.length,
      data: entidades
    });

  } catch (error) {
    console.error('❌ Error obteniendo entidades del pueblo:', error.message);
    res.json({ success: false, error: error.message });
  }
});

/**
 * Buscar entidad por nombre (con normalización)
 */
app.get('/api/firestore/buscar-entidad', async (req, res) => {
  try {
    if (!firestoreDb) {
      return res.json({ success: false, error: 'Firestore no está inicializado' });
    }

    const { nombre, pueblo, categoria } = req.query;
    
    if (!nombre) {
      return res.json({ success: false, error: 'Nombre requerido' });
    }

    const nombreNormalizado = normalizeId(nombre);
    const puebloId = pueblo ? normalizeId(pueblo) : null;
    const categoriaId = categoria ? normalizeId(categoria) : null;

    // Si tenemos pueblo y categoría específicos
    if (puebloId && categoriaId) {
      const entidadDoc = await firestoreDb
        .collection('pueblos')
        .doc(puebloId)
        .collection(categoriaId)
        .doc(nombreNormalizado)
        .get();
      
      if (entidadDoc.exists) {
        return res.json({
          success: true,
          data: {
            id: entidadDoc.id,
            ...entidadDoc.data(),
            pueblo: puebloId,
            categoria: categoriaId
          }
        });
      }
    }

    // Si solo tenemos el pueblo, buscar en todas las categorías
    if (puebloId) {
      const puebloDoc = await firestoreDb.collection('pueblos').doc(puebloId).get();
      
      if (puebloDoc.exists) {
        const categoriasActivas = puebloDoc.data().categorias_activas || [];
        
        for (const cat of categoriasActivas) {
          const catNormalizada = normalizeId(cat);
          
          const entidadDoc = await firestoreDb
            .collection('pueblos')
            .doc(puebloId)
            .collection(catNormalizada)
            .doc(nombreNormalizado)
            .get();
          
          if (entidadDoc.exists) {
            return res.json({
              success: true,
              data: {
                id: entidadDoc.id,
                ...entidadDoc.data(),
                pueblo: puebloId,
                categoria: catNormalizada
              }
            });
          }
        }
      }
    }

    res.json({
      success: false,
      error: 'Entidad no encontrada',
      busqueda: { nombre: nombreNormalizado, pueblo: puebloId, categoria: categoriaId }
    });

  } catch (error) {
    console.error('❌ Error buscando entidad:', error.message);
    res.json({ success: false, error: error.message });
  }
});

// ========================================
// ========================================
// NUEVO: FUNCIONES DE NOTIFICACIONES
// ========================================
// ========================================

/**
 * Enviar notificación a tokens específicos
 */
async function sendNotificationToTokens(tokens, notification, actionData = {}) {
  if (!tokens || tokens.length === 0) {
    return { success: true, total: 0, exitosos: 0, fallidos: 0, details: [] };
  }

  const results = [];
  const chunks = [];
  const CHUNK_SIZE = 500;

  for (let i = 0; i < tokens.length; i += CHUNK_SIZE) {
    chunks.push(tokens.slice(i, i + CHUNK_SIZE));
  }

  for (const chunk of chunks) {
    const message = {
      notification: {
        title: notification.title,
        body: notification.body,
      },
      data: {
        action_type: actionData.action_type || 'open_home',
        action_data: JSON.stringify(actionData),
        timestamp: new Date().toISOString(),
      },
      tokens: chunk,
      android: {
        notification: {
          channelId: 'turisteando_notifications',
          priority: 'high',
          sound: 'default',
        },
        priority: 'high',
      },
    };

    // ✅ IMAGEN PARA IOS - Configuración correcta
    if (notification.image) {
      // Para Android
      message.notification.imageUrl = notification.image;
      
      // Para iOS - APNS con mutable-content
      message.apns = {
        payload: {
          aps: {
            'mutable-content': 1,
            sound: 'default',
          },
        },
        fcmOptions: {
          imageUrl: notification.image,
        },
      };
      
      // También incluir en data para la Notification Service Extension
      message.data.image = notification.image;
    }

    try {
      const response = await admin.messaging().sendEachForMulticast(message);
      
      response.responses.forEach((resp, idx) => {
        results.push({
          token: chunk[idx],
          success: resp.success,
          error: resp.error?.message || null,
        });
      });
    } catch (error) {
      console.error('Error enviando chunk:', error);
      chunk.forEach(token => {
        results.push({
          token,
          success: false,
          error: error.message,
        });
      });
    }
  }

  const exitosos = results.filter(r => r.success).length;
  const fallidos = results.filter(r => !r.success).length;

  return {
    success: true,
    total: tokens.length,
    exitosos,
    fallidos,
    details: results,
  };
}

// ========================================
// SEGMENTACIÓN INTELIGENTE - FUNCIÓN CORREGIDA
// ========================================

/**
 * Obtener tokens con segmentación INTELIGENTE (CORREGIDO)
 * 
 * 🔑 BUG FIX: Si hay entidades en la segmentación, saltar Estrategia 1
 * porque las entidades NO se guardan en preferencias, solo en eventos.
 * 
 * Busca usuarios basándose en:
 * - Preferencias guardadas (método original) - SOLO si NO hay entidades
 * - Pueblos que han visitado (pueblo_view events)
 * - Categorías que han visto (category_view, category_clicked events)
 * - ENTIDADES/SITIOS con los que han interactuado (entity_clicked events)
 * 
 * 🔑 CRÍTICO: Si los tokens no están en device_tokens, los usa directamente desde events
 */
async function getTokensWithSegmentation(segmentation = {}) {
  console.log('🎯 getTokensWithSegmentation llamada con:', JSON.stringify(segmentation));
  
  // 🔑 IMPORTANTE: Detectar si hay entidades en la segmentación
  // Si hay entidades, debemos saltar la Estrategia 1 porque las entidades
  // NO se guardan en preferencias, solo en eventos
  const tieneEntidades = segmentation.entidades && segmentation.entidades.length > 0;
  
  if (tieneEntidades) {
    console.log('🏢 Detectadas entidades en segmentación, saltando Estrategia 1 (preferencias)');
  }
  
  let devices = [];
  
  // ESTRATEGIA 1: Buscar por preferencias guardadas (método original)
  // 🔑 CORRECCIÓN: Solo ejecutar si NO hay entidades en la segmentación
  if (!tieneEntidades) {
    const query = { activo: true };
    
    if (segmentation.pueblos && segmentation.pueblos.length > 0) {
      query['preferencias.pueblos_interes'] = { $in: segmentation.pueblos };
    }
    
    if (segmentation.categorias && segmentation.categorias.length > 0) {
      query['preferencias.categorias_interes'] = { $in: segmentation.categorias };
    }
    
    if (segmentation.device_type) {
      query.device_type = segmentation.device_type;
    }
    
    if (segmentation.active_in_days) {
      const date = new Date();
      date.setDate(date.getDate() - segmentation.active_in_days);
      query.ultimo_acceso = { $gte: date };
    }
    
    if (segmentation.new_users_only) {
      const date = new Date();
      date.setDate(date.getDate() - segmentation.new_users_only);
      query.fecha_registro = { $gte: date };
    }
    
    // Buscar por preferencias guardadas
    devices = await deviceTokensCollection.find(query).toArray();
    console.log(`📊 Usuarios encontrados por preferencias guardadas: ${devices.length}`);
  } else {
    console.log('⏭️ Estrategia 1 omitida porque hay entidades en la segmentación');
  }
  
  // ESTRATEGIA 2: Si no encontramos usuarios por preferencias O hay entidades, buscar por ACTIVIDAD
  // 🔑 CORRECCIÓN: Agregar condición "|| tieneEntidades"
  if ((devices.length === 0 || tieneEntidades) && mongoDb) {
    console.log('🔍 Buscando usuarios por actividad en eventos...');
    
    const tokensPorActividad = new Set();
    
    // ========================================
    // FUNCIÓN HELPER: Generar todas las variantes de un nombre
    // ========================================
    const generateVariants = (name) => {
      if (!name) return [];
      
      const variants = new Set();
      
      // 1. Original
      variants.add(name);
      
      // 2. Lowercase
      variants.add(name.toLowerCase());
      
      // 3. Uppercase
      variants.add(name.toUpperCase());
      
      // 4. Primera letra mayúscula
      variants.add(name.charAt(0).toUpperCase() + name.slice(1).toLowerCase());
      
      // 5. Normalizado (sin tildes, espacios = guiones bajos)
      const normalized = name.toLowerCase()
        .trim()
        .normalize('NFD').replace(/[\u0300-\u036f]/g, '') // Quitar tildes
        .replace(/á/g, 'a').replace(/é/g, 'e').replace(/í/g, 'i')
        .replace(/ó/g, 'o').replace(/ú/g, 'u').replace(/ü/g, 'u')
        .replace(/ñ/g, 'n')
        .replace(/\s+/g, '_')
        .replace(/[^a-z0-9_]/g, '');
      
      variants.add(normalized);
      
      // 6. Normalizado con espacios (villa de leyva)
      variants.add(normalized.replace(/_/g, ' '));
      
      // 7. Normalizado con primera letra mayúscula
      variants.add(normalized.charAt(0).toUpperCase() + normalized.slice(1));
      
      // 8. Con espacios y primera letra mayúscula (Villa de leyva)
      variants.add(normalized.replace(/_/g, ' ').charAt(0).toUpperCase() + normalized.replace(/_/g, ' ').slice(1));
      
      // 9. Título case (Villa De Leyva)
      const titleCase = normalized.replace(/_/g, ' ').split(' ')
        .map(word => word.charAt(0).toUpperCase() + word.slice(1))
        .join(' ');
      variants.add(titleCase);
      
      // 10. Screen name (villa_de_leyvaScreen)
      variants.add(normalized + 'Screen');
      
      // 11. Screen name con título (Villa_De_leyvaScreen)
      variants.add(normalized.charAt(0).toUpperCase() + normalized.slice(1) + 'Screen');
      
      // 12. Sin guiones bajos (villadeleyva)
      variants.add(normalized.replace(/_/g, ''));
      
      // 13. Sin guiones bajos Screen (villadeleyvaScreen)
      variants.add(normalized.replace(/_/g, '') + 'Screen');
      
      return Array.from(variants).filter(v => v && v.length > 0);
    };
    
    // ========================================
    // NUEVO: Buscar usuarios que han interactuado con ENTIDADES/SITIOS específicos
    // ========================================
    if (segmentation.entidades && segmentation.entidades.length > 0) {
      const entidadesBusqueda = segmentation.entidades.flatMap(e => generateVariants(e));
      
      console.log('🏢 Buscando actividad en entidades/sitios:', entidadesBusqueda);
      
      const fechaLimite = new Date();
      fechaLimite.setDate(fechaLimite.getDate() - 90);
      
      const eventosEntidades = await mongoDb.collection('events').aggregate([
        {
          $match: {
            server_time: { $gte: fechaLimite },
            $or: [
              { 'data.entity_id': { $in: entidadesBusqueda } },
              { 'data.entity_name': { $in: entidadesBusqueda } },
              { 'data.entidad': { $in: entidadesBusqueda } },
              { 'data.lugar_nombre': { $in: entidadesBusqueda } },
              { 'data.sitio': { $in: entidadesBusqueda } },
              { 'data.site_name': { $in: entidadesBusqueda } }
            ]
          }
        },
        {
          $group: {
            _id: '$data.device_token',
            eventos: { $sum: 1 },
            entidades: { $addToSet: '$data.entity_name' }
          }
        },
        { $match: { _id: { $ne: null, $ne: '' } } },
        { $sort: { eventos: -1 } },
        { $limit: 500 }
      ]).toArray();
      
      eventosEntidades.forEach(e => {
        if (e._id) tokensPorActividad.add(e._id);
      });
      
      console.log(`📊 Tokens encontrados por actividad en entidades: ${eventosEntidades.length}`);
    }
    
    // ========================================
    // Buscar usuarios que han visitado PUEBLOS específicos (DINÁMICO)
    // ========================================
    if (segmentation.pueblos && segmentation.pueblos.length > 0) {
      // Generar variantes para cada pueblo
      const pueblosBusqueda = segmentation.pueblos.flatMap(p => generateVariants(p));
      
      console.log('🏘️ Buscando actividad en pueblos:', [...new Set(pueblosBusqueda)]);
      
      const fechaLimite = new Date();
      fechaLimite.setDate(fechaLimite.getDate() - 90);
      
      // Generar patrones regex para screen_name dinámicamente
      const screenPatterns = segmentation.pueblos.map(p => {
        const normalized = p.toLowerCase()
          .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
          .replace(/á/g, 'a').replace(/é/g, 'e').replace(/í/g, 'i')
          .replace(/ó/g, 'o').replace(/ú/g, 'u').replace(/ü/g, 'u')
          .replace(/ñ/g, 'n')
          .replace(/\s+/g, '_')
          .replace(/[^a-z0-9_]/g, '');
        return normalized + 'Screen';
      });
      
      const eventosPueblos = await mongoDb.collection('events').aggregate([
        {
          $match: {
            server_time: { $gte: fechaLimite },
            $or: [
              { 'data.pueblo_id': { $in: pueblosBusqueda } },
              { 'data.pueblo_nombre': { $in: pueblosBusqueda } },
              { 'data.pueblo': { $in: pueblosBusqueda } },
              { 'data.town_name': { $in: pueblosBusqueda } },
              { 'data.screen_name': { $in: pueblosBusqueda } },
              // Regex dinámico para screen_name
              { 'data.screen_name': { $regex: new RegExp(screenPatterns.join('|'), 'i') } }
            ]
          }
        },
        {
          $group: {
            _id: '$data.device_token',
            eventos: { $sum: 1 }
          }
        },
        { $match: { _id: { $ne: null, $ne: '' } } },
        { $sort: { eventos: -1 } },
        { $limit: 500 }
      ]).toArray();
      
      eventosPueblos.forEach(e => {
        if (e._id) tokensPorActividad.add(e._id);
      });
      
      console.log(`📊 Tokens encontrados por actividad en pueblos: ${eventosPueblos.length}`);
    }
    
    // ========================================
    // Buscar usuarios que han interactuado con CATEGORÍAS específicas
    // ========================================
    if (segmentation.categorias && segmentation.categorias.length > 0) {
      const categoriasBusqueda = segmentation.categorias.flatMap(c => generateVariants(c));
      
      console.log('📂 Buscando actividad en categorías:', categoriasBusqueda);
      
      const fechaLimite = new Date();
      fechaLimite.setDate(fechaLimite.getDate() - 90);
      
      const eventosCategorias = await mongoDb.collection('events').aggregate([
        {
          $match: {
            server_time: { $gte: fechaLimite },
            $or: [
              { 'data.category_id': { $in: categoriasBusqueda } },
              { 'data.category_name': { $in: categoriasBusqueda } },
              { 'data.categoria': { $in: categoriasBusqueda } },
              { 'data.category': { $in: categoriasBusqueda } }
            ]
          }
        },
        {
          $group: {
            _id: '$data.device_token',
            eventos: { $sum: 1 }
          }
        },
        { $match: { _id: { $ne: null, $ne: '' } } },
        { $sort: { eventos: -1 } },
        { $limit: 500 }
      ]).toArray();
      
      eventosCategorias.forEach(e => {
        if (e._id) tokensPorActividad.add(e._id);
      });
      
      console.log(`📊 Tokens encontrados por actividad en categorías: ${eventosCategorias.length}`);
    }
    
    // ========================================
    // 🔑 CRÍTICO: Si encontramos tokens por actividad, usarlos
    // ========================================
    if (tokensPorActividad.size > 0) {
      const tokensArray = Array.from(tokensPorActividad);
      console.log(`📊 Total tokens únicos por actividad: ${tokensArray.length}`);
      
      // Si ya teníamos dispositivos de la Estrategia 1, combinarlos
      if (devices.length > 0 && !tieneEntidades) {
        const existingTokens = new Set(devices.map(d => d.token));
        const newTokens = tokensArray.filter(t => !existingTokens.has(t));
        
        console.log(`📊 Combinando ${devices.length} de preferencias con ${newTokens.length} de actividad`);
        
        // Buscar info de los nuevos tokens
        if (newTokens.length > 0) {
          const additionalDevices = await deviceTokensCollection.find({
            token: { $in: newTokens },
            activo: true
          }).toArray();
          
          // Agregar tokens que no están en device_tokens
          const foundTokens = new Set(additionalDevices.map(d => d.token));
          newTokens.forEach(token => {
            if (!foundTokens.has(token)) {
              devices.push({
                token: token,
                activo: true,
                source: 'events_direct'
              });
            }
          });
          
          devices.push(...additionalDevices);
        }
      } else {
        // Solo usar tokens de actividad
        devices = await deviceTokensCollection.find({
          token: { $in: tokensArray },
          activo: true
        }).toArray();
        
        console.log(`📊 Tokens en device_tokens: ${devices.length}`);
        
        // 🔑 CRÍTICO: Si no están en device_tokens, usar los tokens de eventos directamente
        if (devices.length === 0) {
          console.log('⚠️ Tokens NO registrados en device_tokens, usando tokens de eventos directamente');
          devices = tokensArray.map(token => ({
            token: token,
            activo: true,
            source: 'events_direct'
          }));
          console.log(`📊 Tokens de eventos utilizados directamente: ${devices.length}`);
        } else if (devices.length < tokensArray.length) {
          // Si algunos están registrados pero otros no, agregar los que faltan
          const registeredTokens = new Set(devices.map(d => d.token));
          const missingTokens = tokensArray.filter(t => !registeredTokens.has(t));
          
          if (missingTokens.length > 0) {
            console.log(`⚠️ Agregando ${missingTokens.length} tokens adicionales desde eventos`);
            missingTokens.forEach(token => {
              devices.push({
                token: token,
                activo: true,
                source: 'events_fallback'
              });
            });
          }
        }
      }
    }
  }
  
  // ESTRATEGIA 3: Búsqueda directa en eventos si aún no hay resultados
  if (devices.length === 0 && mongoDb && (segmentation.pueblos?.length > 0 || segmentation.categorias?.length > 0 || segmentation.entidades?.length > 0)) {
    console.log('🔍 Buscando tokens directamente en eventos...');
    
    const fechaLimite = new Date();
    fechaLimite.setDate(fechaLimite.getDate() - 90);
    
    const orConditions = [];
    
    // Helper para generar variantes (reutilizado)
    const generateVariants = (name) => {
      if (!name) return [];
      
      const variants = new Set();
      variants.add(name);
      variants.add(name.toLowerCase());
      variants.add(name.toUpperCase());
      
      const normalized = name.toLowerCase()
        .trim()
        .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
        .replace(/á/g, 'a').replace(/é/g, 'e').replace(/í/g, 'i')
        .replace(/ó/g, 'o').replace(/ú/g, 'u').replace(/ü/g, 'u')
        .replace(/ñ/g, 'n')
        .replace(/\s+/g, '_')
        .replace(/[^a-z0-9_]/g, '');
      
      variants.add(normalized);
      variants.add(normalized.replace(/_/g, ' '));
      variants.add(normalized + 'Screen');
      
      return Array.from(variants).filter(v => v && v.length > 0);
    };
    
    if (segmentation.pueblos && segmentation.pueblos.length > 0) {
      const screenPatterns = segmentation.pueblos.map(p => {
        const normalized = p.toLowerCase()
          .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
          .replace(/á/g, 'a').replace(/é/g, 'e').replace(/í/g, 'i')
          .replace(/ó/g, 'o').replace(/ú/g, 'u').replace(/ü/g, 'u')
          .replace(/ñ/g, 'n')
          .replace(/\s+/g, '_')
          .replace(/[^a-z0-9_]/g, '');
        return normalized + 'Screen';
      });
      
      segmentation.pueblos.forEach(pueblo => {
        const variants = generateVariants(pueblo);
        variants.forEach(v => {
          orConditions.push({ 'data.pueblo_id': v });
          orConditions.push({ 'data.pueblo_nombre': v });
          orConditions.push({ 'data.town_name': v });
          orConditions.push({ 'data.screen_name': v });
        });
      });
      
      // Regex dinámico para screen_name
      orConditions.push({ 'data.screen_name': { $regex: new RegExp(screenPatterns.join('|'), 'i') } });
    }
    
    if (segmentation.categorias && segmentation.categorias.length > 0) {
      segmentation.categorias.forEach(categoria => {
        const variants = generateVariants(categoria);
        variants.forEach(v => {
          orConditions.push({ 'data.category_id': v });
          orConditions.push({ 'data.category_name': v });
        });
      });
    }
    
    if (segmentation.entidades && segmentation.entidades.length > 0) {
      segmentation.entidades.forEach(entidad => {
        const variants = generateVariants(entidad);
        variants.forEach(v => {
          orConditions.push({ 'data.entity_id': v });
          orConditions.push({ 'data.entity_name': v });
          orConditions.push({ 'data.lugar_nombre': v });
        });
      });
    }
    
    if (orConditions.length > 0) {
      const eventosConToken = await mongoDb.collection('events').aggregate([
        {
          $match: {
            server_time: { $gte: fechaLimite },
            $or: orConditions,
            'data.device_token': { $exists: true, $ne: null, $ne: '' }
          }
        },
        {
          $group: {
            _id: '$data.device_token',
            eventos: { $sum: 1 }
          }
        },
        { $sort: { eventos: -1 } },
        { $limit: 500 }
      ]).toArray();
      
      console.log(`📊 Tokens encontrados directamente en eventos: ${eventosConToken.length}`);
      
      if (eventosConToken.length > 0) {
        const tokensDirectos = eventosConToken.map(e => e._id).filter(t => t);
        
        devices = await deviceTokensCollection.find({
          token: { $in: tokensDirectos },
          activo: true
        }).toArray();
        
        console.log(`📊 Tokens activos encontrados (método directo): ${devices.length}`);
        
        // 🔑 Si no están en device_tokens, usar directamente
        if (devices.length === 0 && tokensDirectos.length > 0) {
          console.log('⚠️ Usando tokens directamente desde eventos (estrategia 3)');
          devices = tokensDirectos.map(token => ({
            token: token,
            activo: true,
            source: 'events_direct_strategy3'
          }));
        }
      }
    }
  }
  
  const tokens = devices.map(d => d.token);
  console.log(`🎯 Tokens finales a enviar: ${tokens.length}`);
  
  return tokens;
}

/**
 * Guardar notificación en historial
 */
async function saveNotificationToHistory(notification, result, targetType = 'all', targetData = {}) {
  try {
    await notificationsHistoryCollection.insertOne({
      titulo: notification.title,
      mensaje: notification.body,
      imagen: notification.image || null,
      action_type: notification.action_type || 'open_home',
      action_data: notification.action_data || {},
      target_type: targetType,
      target_data: targetData,
      tokens_enviados: result.total,
      enviados_exitosos: result.exitosos,
      enviados_fallidos: result.fallidos,
      fecha_envio: new Date(),
      tipo: notification.tipo || 'manual',
    });
  } catch (error) {
    console.error('Error guardando historial:', error);
  }
}

/**
 * Enviar notificación de bienvenida
 */
async function sendWelcomeNotification(token) {
  try {
    const notification = {
      title: '¡Bienvenido a TuristeandoAPP! 🎉',
      body: 'Descubre los pueblos más hermosos de Colombia. ¡Empieza a explorar!',
      tipo: 'welcome'
    };
    
    const actionData = {
      action_type: 'open_home',
    };
    
    const result = await sendNotificationToTokens([token], notification, actionData);
    
    if (result.exitosos > 0) {
      await deviceTokensCollection.updateOne(
        { token },
        { $set: { welcome_sent: true } }
      );
      
      await saveNotificationToHistory(
        notification,
        result,
        'single',
        { token: token.substring(0, 20) + '...' }
      );
      
      console.log('✅ Notificación de bienvenida enviada');
    }
    
    return result;
  } catch (error) {
    console.error('Error enviando bienvenida:', error);
    return { success: false, error: error.message };
  }
}

// ========================================
// NUEVO: CRON JOBS PARA NOTIFICACIONES
// ========================================

const scheduledJobs = new Map();

/**
 * Procesar notificaciones programadas (cada minuto)
 */
async function processScheduledNotifications() {
  try {
    const now = new Date();
    
    const pendingNotifications = await scheduledNotificationsCollection.find({
      sent: false,
      scheduled_date: { $lte: now },
    }).toArray();
    
    for (const notification of pendingNotifications) {
      console.log(`📤 Procesando notificación programada: ${notification.title}`);
      
      let tokens;
      if (notification.target_type === 'all') {
        const devices = await deviceTokensCollection.find({ activo: true }).toArray();
        tokens = devices.map(d => d.token);
      } else {
        tokens = await getTokensWithSegmentation(notification.segmentation || {});
      }
      
      const result = await sendNotificationToTokens(
        tokens,
        { title: notification.title, body: notification.body, image: notification.image },
        { ...notification.action_data, action_type: notification.action_type }
      );
      
      await saveNotificationToHistory(
        { ...notification, tipo: 'scheduled' },
        result,
        notification.target_type,
        notification.segmentation
      );
      
      await scheduledNotificationsCollection.updateOne(
        { _id: notification._id },
        { $set: { sent: true, sent_at: now, result: { exitosos: result.exitosos, fallidos: result.fallidos } } }
      );
    }
    
  } catch (error) {
    console.error('Error procesando notificaciones programadas:', error);
  }
}

/**
 * Programar job recurrente
 */
function scheduleRecurringJob(id, config) {
  if (scheduledJobs.has(id)) {
    scheduledJobs.get(id).stop();
  }
  
  const job = cron.schedule(config.cron_expression, async () => {
    console.log(`🔄 Ejecutando notificación recurrente: ${config.name}`);
    
    const now = new Date();
    if (config.end_date && now > new Date(config.end_date)) {
      job.stop();
      scheduledJobs.delete(id);
      return;
    }
    if (config.start_date && now < new Date(config.start_date)) {
      return;
    }
    
    let tokens;
    if (config.target_type === 'all') {
      const devices = await deviceTokensCollection.find({ activo: true }).toArray();
      tokens = devices.map(d => d.token);
    } else {
      tokens = await getTokensWithSegmentation(config.segmentation || {});
    }
    
    const result = await sendNotificationToTokens(
      tokens,
      { title: config.title, body: config.body, image: config.image },
      { ...config.action_data, action_type: config.action_type }
    );
    
    await saveNotificationToHistory(
      { ...config, tipo: 'recurring' },
      result,
      config.target_type,
      config.segmentation
    );
    
    await recurringNotificationsCollection.updateOne(
      { _id: new ObjectId(id) },
      { 
        $set: { last_run: now },
        $inc: { total_runs: 1 }
      }
    );
    
  }, {
    timezone: config.timezone || 'America/Bogota',
  });
  
  scheduledJobs.set(id, job);
}

/**
 * Cargar jobs recurrentes al iniciar
 */
async function loadRecurringJobs() {
  try {
    const recurring = await recurringNotificationsCollection.find({ active: true }).toArray();
    
    for (const config of recurring) {
      scheduleRecurringJob(config._id.toString(), config);
    }
    
    console.log(`✅ ${recurring.length} notificaciones recurrentes cargadas`);
  } catch (error) {
    console.error('Error cargando jobs recurrentes:', error);
  }
}

// ========================================
// INICIAR CRON JOBS
// ========================================

// Procesar notificaciones programadas cada minuto
cron.schedule('* * * * *', processScheduledNotifications);
console.log('✅ Cron de notificaciones programadas iniciado');

// Cargar jobs recurrentes después de conectar MongoDB
setTimeout(loadRecurringJobs, 2000);

// ========================================
// BASIC ENDPOINTS (tu código existente)
// ========================================

app.get('/api/health', (req, res) => {
  res.json({ 
    status: 'OK', 
    timestamp: new Date().toISOString(),
    propertyId: PROPERTY_ID,
    mongodb: mongoDb ? 'connected' : 'disconnected',
    firebaseAdmin: admin.apps.length > 0 ? 'initialized' : 'not initialized',
    firestore: firestoreDb ? 'initialized' : 'not initialized',
    version: '5.6.0-segmentacion-entidades-fix'
  });
});

// ========================================
// TODOS TUS ENDPOINTS EXISTENTES SIN CAMBIOS
// ========================================

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
// ENDPOINTS DINÁMICOS - TURISTEANDO APP (tu código existente)
// ========================================

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

// VISTA DE CATEGORÍAS
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

// DETALLES DE ENTIDADES
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
// MONGODB ATLAS - ENDPOINTS (tu código existente)
// ========================================

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
// DASHBOARD MONGODB - JERÁRQUICO INTELIGENTE (tu código existente)
// ========================================

app.get('/api/mongodb/dashboard', async (req, res) => {
  try {
    if (!mongoDb) {
      return res.json({ success: false, error: 'MongoDB no está conectado' });
    }
    
    const { days = 7 } = req.query;
    const startDate = new Date();
    startDate.setDate(startDate.getDate() - parseInt(days));

    const [
      totalEvents,
      eventsByType,
      pueblosViews,
      categoriasData,
      entidadesData,
      accionesData,
      eventsByDay
    ] = await Promise.all([
      mongoDb.collection('events').countDocuments({ server_time: { $gte: startDate } }),
      
      mongoDb.collection('events').aggregate([
        { $match: { server_time: { $gte: startDate } } },
        { $group: { _id: '$event_name', count: { $sum: 1 } } },
        { $sort: { count: -1 } },
        { $limit: 20 }
      ]).toArray(),
      
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
      
      mongoDb.collection('events').aggregate([
        { $match: { 
          server_time: { $gte: startDate },
          event_name: { $in: ['entity_action', 'abrir_mapa', 'abrir_informacion', 'social_network_open', 'whatsapp_open', 'phone_call', 'share', 'favorite_toggled', 'favorito_agregado', 'favorito_eliminado'] }
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
          { case: { $eq: ['$event_name', 'share'] }, then: 'compartir' },
          { case: { $eq: ['$event_name', 'favorite_toggled'] }, then: 'favorito' },
          { case: { $eq: ['$event_name', 'favorito_agregado'] }, then: 'favorito_agregado' },
          { case: { $eq: ['$event_name', 'favorito_eliminado'] }, then: 'favorito_eliminado' }
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
      
      mongoDb.collection('events').aggregate([
        { $match: { server_time: { $gte: startDate } } },
        { $group: { _id: '$date', count: { $sum: 1 }, devices: { $addToSet: '$data.device_model' } } },
        { $project: { _id: 0, date: '$_id', count: 1, devices: { $size: '$devices' } } },
        { $sort: { date: 1 } }
      ]).toArray()
    ]);

    const categoryCorrections = calculateCorrectCategories(entidadesData);

    const pueblosMap = new Map();
    
    const normalizePueblo = (nombre) => {
      if (!nombre) return 'sin_pueblo';
      const normalized = nombre.toString().toLowerCase().trim();
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
    
    entidadesData.forEach(e => {
      const puebloKey = normalizePueblo(e._id.pueblo);
      const categoriaOriginal = e._id.categoria || 'sin_categoria';
      const entidadNombre = e._id.entidad;
      
      if (!entidadNombre) return;
      
      const entityKey = `${entidadNombre}|${puebloKey}`;
      const correction = categoryCorrections.get(entityKey);
      const categoriaNombre = correction ? correction.categoriaCorrecta : categoriaOriginal;
      
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
      
      const entidadKey = entidadNombre;
      
      if (!pueblo.entidades.has(entidadKey)) {
        pueblo.entidades.set(entidadKey, {
          nombre: entidadNombre,
          categoria: categoriaNombre,
          clicks: 0,
          acciones: []
        });
      }
      pueblo.entidades.get(entidadKey).clicks += e.clicks;
      
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
    
    accionesData.forEach(a => {
      const puebloKey = normalizePueblo(a._id.pueblo);
      const entidadNombre = a._id.entity;
      const categoriaOriginal = a._id.categoria || 'sin_categoria';
      const actionName = a._id.action;
      
      if (!entidadNombre || !actionName) return;
      
      const entityKey = `${entidadNombre}|${puebloKey}`;
      const correction = categoryCorrections.get(entityKey);
      const categoriaNombre = correction ? correction.categoriaCorrecta : categoriaOriginal;
      
      if (pueblosMap.has(puebloKey)) {
        const pueblo = pueblosMap.get(puebloKey);
        const entidadKey = entidadNombre;
        
        if (pueblo.entidades.has(entidadKey)) {
          const entidad = pueblo.entidades.get(entidadKey);
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

    res.json({
      success: true,
      data: {
        resumen: {
          totalEventos: totalEvents,
          totalPueblos: pueblosJerarquico.length,
          totalCategorias: totalCategorias.size,
          totalEntidades: totalEntidades.size,
          totalAcciones: accionesData.reduce((sum, a) => sum + a.count, 0),
          periodo: `Últimos ${days} días`,
          modoInteligente: true
        },
        pueblos: pueblosJerarquico,
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

// ENDPOINT PARA VER CORRECCIONES (DEBUG)
app.get('/api/mongodb/debug/corrections', async (req, res) => {
  try {
    if (!mongoDb) {
      return res.json({ success: false, error: 'MongoDB no está conectado' });
    }
    
    const { days = 7 } = req.query;
    const startDate = new Date();
    startDate.setDate(startDate.getDate() - parseInt(days));
    
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
    
    const corrections = calculateCorrectCategories(entidadesData);
    
    const listaCorrecciones = [];
    corrections.forEach((info, entityKey) => {
      const [nombre, pueblo] = entityKey.split('|');
      const categoriasArray = Object.entries(info.detalles).map(([cat, count]) => ({
        categoria: cat,
        eventos: count
      })).sort((a, b) => b.eventos - a.eventos);
      
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
// NUEVOS ENDPOINTS DE NOTIFICACIONES PRO
// ========================================
// ========================================

/**
 * Registrar token de dispositivo (ACTUALIZADO con bienvenida automática)
 */
app.post('/api/notifications/register', async (req, res) => {
  try {
    if (!mongoDb) {
      return res.json({ success: false, error: 'MongoDB no está conectado' });
    }
    
    const { token, device_type = 'android', device_info = {} } = req.body;
    
    if (!token) {
      return res.json({ success: false, error: 'Token requerido' });
    }
    
    const existingDevice = await deviceTokensCollection.findOne({ token });
    
    if (existingDevice) {
      await deviceTokensCollection.updateOne(
        { token },
        { 
          $set: { 
            ultimo_acceso: new Date(),
            device_type,
            device_info,
            activo: true,
          } 
        }
      );
      
      return res.json({ 
        success: true, 
        message: 'Token actualizado',
        is_new: false,
      });
    }
    
    // Nuevo dispositivo
    await deviceTokensCollection.insertOne({
      token,
      device_type,
      device_info,
      fecha_registro: new Date(),
      ultimo_acceso: new Date(),
      activo: true,
      preferencias: {
        pueblos_interes: [],
        categorias_interes: [],
        notificaciones_activas: true,
      },
      welcome_sent: false,
    });
    
    // Enviar notificación de bienvenida automáticamente
    if (process.env.WELCOME_NOTIFICATION !== 'false') {
      setTimeout(async () => {
        await sendWelcomeNotification(token);
      }, 3000);
    }
    
    res.json({ 
      success: true, 
      message: 'Token registrado exitosamente',
      is_new: true,
    });
    
  } catch (error) {
    console.error('Error registrando token:', error);
    res.json({ success: false, error: error.message });
  }
});

/**
 * Actualizar preferencias de usuario (NUEVO)
 */
app.post('/api/notifications/preferences', async (req, res) => {
  try {
    if (!mongoDb) {
      return res.json({ success: false, error: 'MongoDB no está conectado' });
    }
    
    const { token, pueblos_interes = [], categorias_interes = [], notificaciones_activas = true } = req.body;
    
    if (!token) {
      return res.json({ success: false, error: 'Token requerido' });
    }
    
    await deviceTokensCollection.updateOne(
      { token },
      {
        $set: {
          'preferencias.pueblos_interes': pueblos_interes,
          'preferencias.categorias_interes': categorias_interes,
          'preferencias.notificaciones_activas': notificaciones_activas,
          'preferencias.actualizado': new Date(),
        }
      },
      { upsert: true }
    );
    
    res.json({ success: true, message: 'Preferencias actualizadas' });
    
  } catch (error) {
    console.error('Error actualizando preferencias:', error);
    res.json({ success: false, error: error.message });
  }
});

/**
 * Enviar notificación (ACTUALIZADO con segmentación inteligente)
 */
app.post('/api/notifications/send', async (req, res) => {
  try {
    if (!admin.apps.length) {
      return res.json({ success: false, error: 'Firebase Admin no está inicializado' });
    }
    
    if (!mongoDb) {
      return res.json({ success: false, error: 'MongoDB no está conectido' });
    }
    
    const { 
      title, 
      body, 
      image,
      target_type = 'all',
      segmentation = {},
      action_type = 'open_home',
      action_data = {},
    } = req.body;
    
    if (!title || !body) {
      return res.json({ success: false, error: 'Título y mensaje requeridos' });
    }
    
    // Obtener tokens según segmentación
    let tokens;
    
    if (target_type === 'all') {
      const devices = await deviceTokensCollection.find({ activo: true }).toArray();
      tokens = devices.map(d => d.token);
    } else if (target_type === 'segmented') {
      tokens = await getTokensWithSegmentation(segmentation);
    } else if (target_type === 'test') {
      tokens = ['test_token'];
    } else {
      const devices = await deviceTokensCollection.find({ activo: true }).toArray();
      tokens = devices.map(d => d.token);
    }
    
    if (tokens.length === 0) {
      return res.json({ 
        success: true, 
        message: 'No hay dispositivos registrados con esa segmentación',
        stats: { total: 0, exitosos: 0, fallidos: 0 }
      });
    }
    
    const notification = { title, body, image };
    const fullActionData = { ...action_data, action_type };
    
    const result = await sendNotificationToTokens(tokens, notification, fullActionData);
    
    // Guardar en historial
    await saveNotificationToHistory(
      { ...notification, action_type, action_data, tipo: 'manual' },
      result,
      target_type,
      segmentation
    );
    
    res.json({
      success: true,
      message: 'Notificación enviada',
      stats: {
        total: result.total,
        exitosos: result.exitosos,
        fallidos: result.fallidos,
      },
      segmentation_usada: segmentation,
      details: result.details.slice(0, 5),
    });
    
  } catch (error) {
    console.error('Error enviando notificación:', error);
    res.json({ success: false, error: error.message });
  }
});

/**
 * Programar notificación (CON CORRECCIÓN DE ZONA HORARIA)
 */
app.post('/api/notifications/schedule', async (req, res) => {
  try {
    if (!mongoDb) {
      return res.json({ success: false, error: 'MongoDB no está conectado' });
    }
    
    const {
      title,
      body,
      image,
      scheduled_date,
      timezone = 'America/Bogota',
      target_type = 'all',
      segmentation = {},
      action_type = 'open_home',
      action_data = {},
    } = req.body;
    
    if (!title || !body || !scheduled_date) {
      return res.status(400).json({ 
        success: false, 
        error: 'Título, mensaje y fecha programada son requeridos' 
      });
    }
    
    // ========================================
    // CORRECCIÓN DE ZONA HORARIA PARA COLOMBIA
    // ========================================
    let scheduledDate;
    
    const hasTimezone = scheduled_date.includes('Z') || 
                        scheduled_date.includes('+') || 
                        (scheduled_date.lastIndexOf('-') > 10);
    
    if (!hasTimezone) {
      const dateWithTimezone = scheduled_date + '-05:00';
      scheduledDate = new Date(dateWithTimezone);
      console.log(`📅 Fecha recibida: ${scheduled_date} (Colombia UTC-5)`);
      console.log(`📅 Convertida a UTC: ${scheduledDate.toISOString()}`);
    } else {
      scheduledDate = new Date(scheduled_date);
    }
    
    if (isNaN(scheduledDate.getTime())) {
      return res.status(400).json({ 
        success: false, 
        error: 'Formato de fecha inválido' 
      });
    }
    
    const now = new Date();
    const minTime = new Date(now.getTime() + 60 * 1000);
    
    if (scheduledDate < minTime) {
      return res.status(400).json({ 
        success: false, 
        error: 'La fecha programada debe ser al menos 1 minuto en el futuro' 
      });
    }
    
    const scheduledNotification = {
      title,
      body,
      image: image || null,
      scheduled_date: scheduledDate,
      timezone,
      target_type,
      segmentation,
      action_type,
      action_data,
      sent: false,
      created_at: new Date(),
    };
    
    const result = await scheduledNotificationsCollection.insertOne(scheduledNotification);
    
    res.json({
      success: true,
      message: 'Notificación programada correctamente',
      scheduled_id: result.insertedId,
      scheduled_date: scheduledDate.toISOString(),
      timezone_applied: 'America/Bogota (UTC-5)'
    });
    
  } catch (error) {
    console.error('Error programando notificación:', error);
    res.json({ success: false, error: error.message });
  }
});

/**
 * Obtener notificaciones programadas
 */
app.get('/api/notifications/scheduled', async (req, res) => {
  try {
    if (!mongoDb) {
      return res.json({ success: false, error: 'MongoDB no está conectado' });
    }
    
    const scheduled = await scheduledNotificationsCollection
      .find({ sent: false })
      .sort({ scheduled_date: 1 })
      .toArray();
    
    res.json({ success: true, data: scheduled });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

/**
 * Cancelar notificación programada
 */
app.delete('/api/notifications/scheduled/:id', async (req, res) => {
  try {
    if (!mongoDb) {
      return res.json({ success: false, error: 'MongoDB no está conectado' });
    }
    
    const result = await scheduledNotificationsCollection.deleteOne({
      _id: new ObjectId(req.params.id),
      sent: false,
    });
    
    if (result.deletedCount > 0) {
      res.json({ success: true, message: 'Notificación cancelada' });
    } else {
      res.status(404).json({ success: false, error: 'Notificación no encontrada o ya enviada' });
    }
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

/**
 * Crear notificación recurrente
 */
app.post('/api/notifications/recurring', async (req, res) => {
  try {
    if (!mongoDb) {
      return res.json({ success: false, error: 'MongoDB no está conectado' });
    }
    
    const{
      name,
      title,
      body,
      image,
      cron_expression,
      timezone = 'America/Bogota',
      target_type = 'all',
      segmentation = {},
      action_type = 'open_home',
      action_data = {},
      start_date,
      end_date,
    } = req.body;
    
    if (!name || !title || !body || !cron_expression) {
      return res.status(400).json({ 
        success: false, 
        error: 'Nombre, título, mensaje y expresión cron son requeridos' 
      });
    }
    
    if (!cron.validate(cron_expression)) {
      return res.status(400).json({ 
        success: false, 
        error: 'Expresión cron inválida' 
      });
    }
    
    const recurringNotification = {
      name,
      title,
      body,
      image: image || null,
      cron_expression,
      timezone,
      target_type,
      segmentation,
      action_type,
      action_data,
      start_date: start_date ? new Date(start_date) : null,
      end_date: end_date ? new Date(end_date) : null,
      active: true,
      created_at: new Date(),
      last_run: null,
      next_run: null,
      total_runs: 0,
    };
    
    const result = await recurringNotificationsCollection.insertOne(recurringNotification);
    
    scheduleRecurringJob(result.insertedId.toString(), recurringNotification);
    
    res.json({
      success: true,
      message: 'Notificación recurrente creada',
      recurring_id: result.insertedId,
      cron_expression,
    });
    
  } catch (error) {
    console.error('Error creando notificación recurrente:', error);
    res.json({ success: false, error: error.message });
  }
});

/**
 * Obtener notificaciones recurrentes
 */
app.get('/api/notifications/recurring', async (req, res) => {
  try {
    if (!mongoDb) {
      return res.json({ success: false, error: 'MongoDB no está conectado' });
    }
    
    const recurring = await recurringNotificationsCollection.find({}).toArray();
    res.json({ success: true, data: recurring });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

/**
 * Activar/Desactivar notificación recurrente
 */
app.patch('/api/notifications/recurring/:id/toggle', async (req, res) => {
  try {
    if (!mongoDb) {
      return res.json({ success: false, error: 'MongoDB no está conectado' });
    }
    
    const recurring = await recurringNotificationsCollection.findOne({
      _id: new ObjectId(req.params.id),
    });
    
    if (!recurring) {
      return res.status(404).json({ success: false, error: 'Notificación no encontrada' });
    }
    
    const newStatus = !recurring.active;
    
    await recurringNotificationsCollection.updateOne(
      { _id: new ObjectId(req.params.id) },
      { $set: { active: newStatus } }
    );
    
    if (newStatus) {
      scheduleRecurringJob(req.params.id, recurring);
    }
    
    res.json({ 
      success: true, 
      message: newStatus ? 'Notificación activada' : 'Notificación desactivada',
      active: newStatus,
    });
    
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

/**
 * Eliminar notificación recurrente
 */
app.delete('/api/notifications/recurring/:id', async (req, res) => {
  try {
    if (!mongoDb) {
      return res.json({ success: false, error: 'MongoDB no está conectado' });
    }
    
    await recurringNotificationsCollection.deleteOne({
      _id: new ObjectId(req.params.id),
    });
    res.json({ success: true, message: 'Notificación recurrente eliminada' });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

/**
 * Historial de notificaciones
 */
app.get('/api/notifications/history', async (req, res) => {
  try {
    if (!mongoDb) {
      return res.json({ success: false, error: 'MongoDB no está conectado' });
    }
    
    const { limit = 20 } = req.query;
    
    const history = await notificationsHistoryCollection
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
 * Estadísticas de notificaciones
 */
app.get('/api/notifications/stats', async (req, res) => {
  try {
    if (!mongoDb) {
      return res.json({ success: false, error: 'MongoDB no está conectado' });
    }
    
    const totalDevices = await deviceTokensCollection.countDocuments({ activo: true });
    const totalNotifications = await notificationsHistoryCollection.countDocuments();
    
    const last30Days = new Date();
    last30Days.setDate(last30Days.getDate() - 30);
    
    const notifications30Days = await notificationsHistoryCollection.find({
      fecha_envio: { $gte: last30Days }
    }).toArray();
    
    const totalExitosos30Dias = notifications30Days.reduce((sum, n) => sum + (n.enviados_exitosos || 0), 0);
    
    const scheduledCount = await scheduledNotificationsCollection.countDocuments({ sent: false });
    const recurringCount = await recurringNotificationsCollection.countDocuments({ active: true });
    
    const devicesByType = await deviceTokensCollection.aggregate([
      { $match: { activo: true } },
      { $group: { _id: '$device_type', count: { $sum: 1 } } }
    ]).toArray();
    
    const popularPueblos = await deviceTokensCollection.aggregate([
      { $unwind: '$preferencias.pueblos_interes' },
      { $group: { _id: '$preferencias.pueblos_interes', count: { $sum: 1 } } },
      { $sort: { count: -1 } },
      { $limit: 5 }
    ]).toArray();
    
    const popularCategories = await deviceTokensCollection.aggregate([
      { $unwind: '$preferencias.categorias_interes' },
      { $group: { _id: '$preferencias.categorias_interes', count: { $sum: 1 } } },
      { $sort: { count: -1 } },
      { $limit: 5 }
    ]).toArray();
    
    res.json({
      success: true,
      data: {
        dispositivosRegistrados: totalDevices,
        notificacionesEnviadas: totalNotifications,
        totalExitosos30Dias,
        notificacionesProgramadas: scheduledCount,
        notificacionesRecurrentes: recurringCount,
        dispositivosPorTipo: devicesByType.reduce((acc, d) => {
          acc[d._id || 'unknown'] = d.count;
          return acc;
        }, {}),
        pueblosPopulares: popularPueblos.map(p => ({ nombre: p._id, usuarios: p.count })),
        categoriasPopulares: popularCategories.map(c => ({ nombre: c._id, usuarios: c.count })),
      }
    });
    
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// ========================================
// ENDPOINT: dynamic-data que lee desde Firestore
// ========================================
app.get('/api/notifications/dynamic-data', async (req, res) => {
  try {
    if (firestoreDb) {
      console.log('📊 Leyendo datos dinámicos desde Firestore...');
      
      const pueblosSnapshot = await firestoreDb.collection('pueblos').get();
      
      const pueblos = [];
      const categoriasSet = new Set();
      const entidades = [];
      
      for (const doc of pueblosSnapshot.docs) {
        const data = doc.data();
        const puebloId = doc.id;
        const nombrePueblo = data.nombre || puebloId;
        const categoriasActivas = data.categorias_activas || [];
        
        pueblos.push({
          id: puebloId,
          nombre: nombrePueblo,
          categorias_activas: categoriasActivas
        });
        
        categoriasActivas.forEach(cat => categoriasSet.add(cat));
        
        try {
          const subcollections = await firestoreDb
            .collection('pueblos')
            .doc(puebloId)
            .listCollections();
          
          for (const subcol of subcollections) {
            categoriasSet.add(subcol.id);
          }
        } catch (listError) {
          console.log(`  ⚠️ No se pudieron listar subcolecciones de ${puebloId}`);
        }
      }

      const categorias = Array.from(categoriasSet)
        .sort()
        .map(cat => ({
          id: normalizeId(cat),
          nombre: cat.charAt(0).toUpperCase() + cat.slice(1).replace(/_/g, ' '),
          original: cat
        }));

      for (const pueblo of pueblos) {
        const categoriasDelPueblo = Array.from(categoriasSet);
        
        for (const categoria of categoriasDelPueblo) {
          const categoriaNormalizada = normalizeId(categoria);
          
          try {
            const entidadesSnapshot = await firestoreDb
              .collection('pueblos')
              .doc(pueblo.id)
              .collection(categoriaNormalizada)
              .get();
            
            if (!entidadesSnapshot.empty) {
              entidadesSnapshot.forEach(doc => {
                const entityData = doc.data();
                entidades.push({
                  id: doc.id,
                  nombre: entityData.nombre || doc.id,
                  categoria: categoriaNormalizada,
                  pueblo: pueblo.id
                });
              });
            }
          } catch (e) {}
        }
      }

      console.log(`📊 Datos desde Firestore: ${pueblos.length} pueblos, ${categorias.length} categorías, ${entidades.length} entidades`);

      return res.json({
        success: true,
        source: 'firestore',
        data: {
          pueblos,
          categorias,
          entidades,
          total_categorias: categorias.length,
          total_pueblos: pueblos.length,
          total_entidades: entidades.length
        }
      });
    }
    
    // Fallback a MongoDB
    if (!mongoDb) {
      return res.json({ success: false, error: 'Ni Firestore ni MongoDB están disponibles' });
    }
    
    const { days = 30 } = req.query;
    const startDate = new Date();
    startDate.setDate(startDate.getDate() - parseInt(days));
    
    const pueblos = await mongoDb.collection('events').aggregate([
      { $match: { server_time: { $gte: startDate } } },
      { $project: { pueblo: { $ifNull: ['$data.pueblo_nombre', '$data.pueblo_id'] } } },
      { $match: { pueblo: { $ne: null, $ne: '' } } },
      { $group: { _id: '$pueblo', count: { $sum: 1 } } },
      { $sort: { count: -1 } }
    ]).toArray();
    
    const categoriasMongo = await mongoDb.collection('events').aggregate([
      { $match: { server_time: { $gte: startDate } } },
      { $project: { categoria: { $ifNull: ['$data.category_name', '$data.category_id'] } } },
      { $match: { categoria: { $ne: null, $ne: '' } } },
      { $group: { _id: '$categoria', count: { $sum: 1 } } },
      { $sort: { count: -1 } },
      { $limit: 20 }
    ]).toArray();
    
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
    
    res.json({
      success: true,
      source: 'mongodb',
      data: {
        pueblos: pueblos.map(p => ({ id: normalizeId(p._id), nombre: p._id })),
        categorias: categoriasMongo.map(c => ({ id: normalizeId(c._id), nombre: c._id })),
        entidades: entidades.map(e => ({
          id: normalizeId(e._id.entidad),
          nombre: e._id.entidad,
          categoria: e._id.categoria,
          pueblo: e._id.pueblo
        })),
      }
    });
    
  } catch (error) {
    console.error('❌ Error en dynamic-data:', error.message);
    res.json({ success: false, error: error.message });
  }
});

/**
 * Test de notificación
 */
app.get('/api/notifications/test-send', async (req, res) => {
  try {
    if (!admin.apps.length) {
      return res.json({ success: false, error: 'Firebase Admin no está inicializado' });
    }
    
    if (!mongoDb) {
      return res.json({ success: false, error: 'MongoDB no está conectado' });
    }
    
    const devices = await deviceTokensCollection.find({ activo: true }).toArray();
    const tokens = devices.map(d => d.token);
    
    if (tokens.length === 0) {
      return res.json({ success: false, error: 'No hay dispositivos registrados' });
    }
    
    const notification = {
      title: 'TuristeandoAPP',
      body: 'Esta es una notificación de prueba',
    };
    
    const result = await sendNotificationToTokens(tokens, notification, { action_type: 'open_home' });
    
    await saveNotificationToHistory(
      { ...notification, tipo: 'manual' },
      result,
      'test',
      {}
    );
    
    res.json({
      success: true,
      message: 'Notificación enviada',
      stats: {
        total: result.total,
        exitosos: result.exitosos,
        fallidos: result.fallidos,
      },
      details: result.details,
    });
    
  } catch (error) {
    console.error('Error en test-send:', error);
    res.json({ success: false, error: error.message });
  }
});

/**
 * Eliminar token
 */
app.delete('/api/notifications/token/:token', async (req, res) => {
  try {
    if (!mongoDb) {
      return res.json({ success: false, error: 'MongoDB no está conectado' });
    }
    
    const { token } = req.params;
    
    await deviceTokensCollection.deleteOne({ token });
    
    res.json({ success: true, message: 'Token eliminado' });
    
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// ========================================
// NUEVO: ENDPOINT PARA DEBUG DE SEGMENTACIÓN
// ========================================

/**
 * Debug de segmentación - ver qué tokens se encontrarían
 */
app.post('/api/notifications/debug-segmentation', async (req, res) => {
  try {
    if (!mongoDb) {
      return res.json({ success: false, error: 'MongoDB no está conectado' });
    }
    
    const { segmentation = {} } = req.body;
    
    console.log('🔍 Debug de segmentación:', JSON.stringify(segmentation));
    
    // 🔑 Detectar si hay entidades
    const tieneEntidades = segmentation.entidades && segmentation.entidades.length > 0;
    
    // ESTRATEGIA 1: Por preferencias guardadas (solo si NO hay entidades)
    const query = { activo: true };
    
    if (segmentation.pueblos && segmentation.pueblos.length > 0) {
      query['preferencias.pueblos_interes'] = { $in: segmentation.pueblos };
    }
    
    if (segmentation.categorias && segmentation.categorias.length > 0) {
      query['preferencias.categorias_interes'] = { $in: segmentation.categorias };
    }
    
    let devicesByPreferences = [];
    if (!tieneEntidades) {
      devicesByPreferences = await deviceTokensCollection.find(query).toArray();
    }
    
    // ESTRATEGIA 2: Por actividad en eventos
    const tokensPorActividad = new Set();
    let eventosPueblos = [];
    let eventosCategorias = [];
    let eventosEntidades = [];
    
    const fechaLimite = new Date();
    fechaLimite.setDate(fechaLimite.getDate() - 90);
    
    if (segmentation.pueblos && segmentation.pueblos.length > 0) {
      const pueblosBusqueda = segmentation.pueblos.map(p => {
        const normalized = normalizeId(p);
        return [normalized, p.toLowerCase(), p];
      }).flat();
      
      eventosPueblos = await mongoDb.collection('events').aggregate([
        {
          $match: {
            server_time: { $gte: fechaLimite },
            $or: [
              { 'data.pueblo_id': { $in: pueblosBusqueda } },
              { 'data.pueblo_nombre': { $in: pueblosBusqueda } }
            ]
          }
        },
        {
          $group: {
            _id: '$data.device_token',
            eventos: { $sum: 1 }
          }
        },
        { $match: { _id: { $ne: null, $ne: '' } } },
        { $sort: { eventos: -1 } },
        { $limit: 10 }
      ]).toArray();
      
      eventosPueblos.forEach(e => {
        if (e._id) tokensPorActividad.add(e._id);
      });
    }
    
    if (segmentation.categorias && segmentation.categorias.length > 0) {
      const categoriasBusqueda = segmentation.categorias.map(c => {
        const normalized = normalizeId(c);
        return [normalized, c.toLowerCase(), c];
      }).flat();
      
      eventosCategorias = await mongoDb.collection('events').aggregate([
        {
          $match: {
            server_time: { $gte: fechaLimite },
            $or: [
              { 'data.category_id': { $in: categoriasBusqueda } },
              { 'data.category_name': { $in: categoriasBusqueda } }
            ]
          }
        },
        {
          $group: {
            _id: '$data.device_token',
            eventos: { $sum: 1 }
          }
        },
        { $match: { _id: { $ne: null, $ne: '' } } },
        { $sort: { eventos: -1 } },
        { $limit: 10 }
      ]).toArray();
      
      eventosCategorias.forEach(e => {
        if (e._id) tokensPorActividad.add(e._id);
      });
    }
    
    // NUEVO: Buscar por entidades
    if (segmentation.entidades && segmentation.entidades.length > 0) {
      const entidadesBusqueda = segmentation.entidades.map(e => {
        const normalized = normalizeId(e);
        return [normalized, e.toLowerCase(), e];
      }).flat();
      
      eventosEntidades = await mongoDb.collection('events').aggregate([
        {
          $match: {
            server_time: { $gte: fechaLimite },
            $or: [
              { 'data.entity_id': { $in: entidadesBusqueda } },
              { 'data.entity_name': { $in: entidadesBusqueda } },
              { 'data.lugar_nombre': { $in: entidadesBusqueda } }
            ]
          }
        },
        {
          $group: {
            _id: '$data.device_token',
            eventos: { $sum: 1 }
          }
        },
        { $match: { _id: { $ne: null, $ne: '' } } },
        { $sort: { eventos: -1 } },
        { $limit: 10 }
      ]).toArray();
      
      eventosEntidades.forEach(e => {
        if (e._id) tokensPorActividad.add(e._id);
      });
    }
    
    const tokensArray = Array.from(tokensPorActividad);
    const devicesByActivity = tokensArray.length > 0 
      ? await deviceTokensCollection.find({
          token: { $in: tokensArray },
          activo: true
        }).toArray()
      : [];
    
    res.json({
      success: true,
      debug: {
        segmentation,
        tieneEntidades,
        estrategia1_preferencias: {
          query,
          ejecutada: !tieneEntidades,
          encontrados: devicesByPreferences.length,
          tokens: devicesByPreferences.slice(0, 5).map(d => d.token?.substring(0, 30) + '...')
        },
        estrategia2_actividad: {
          pueblos_buscados: segmentation.pueblos || [],
          categorias_buscadas: segmentation.categorias || [],
          entidades_buscadas: segmentation.entidades || [],
          eventos_pueblos: eventosPueblos.length,
          eventos_categorias: eventosCategorias.length,
          eventos_entidades: eventosEntidades.length,
          tokens_unicos: tokensArray.length,
          tokens_activos: devicesByActivity.length,
          tokens_ejemplo: tokensArray.slice(0, 3).map(t => t?.substring(0, 30) + '...')
        }
      },
      resumen: {
        total_por_preferencias: devicesByPreferences.length,
        total_por_actividad: devicesByActivity.length,
        total_final: tieneEntidades 
          ? devicesByActivity.length 
          : (devicesByPreferences.length > 0 ? devicesByPreferences.length : devicesByActivity.length)
      }
    });
    
  } catch (error) {
    console.error('Error en debug-segmentation:', error);
    res.json({ success: false, error: error.message });
  }
});



// ========================================
// INICIAR SERVIDOR
// ========================================

app.listen(PORT, () => {
  console.log(`🚀 Turisteando Analytics Server v5.6.0 (Segmentación Entidades FIX) running on port ${PORT}`);
  console.log(`📊 Firebase Property ID: ${PROPERTY_ID}`);
  console.log(`🍃 MongoDB: ${mongoDb ? 'Conectado' : 'No configurado'}`);
  console.log(`🔔 Firebase Admin: ${admin.apps.length > 0 ? 'Inicializado' : 'No configurado'}`);
  console.log(`🔥 Firestore: ${firestoreDb ? 'Inicializado' : 'No configurado'}`);
});
