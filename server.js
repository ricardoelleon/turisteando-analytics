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

// Top events (general)
app.get('/api/analytics/top-events', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    
    const report = await runReport(['eventName'], ['eventCount', 'activeUsers'], { startDate, endDate }, 'eventCount', 30);
    
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

// ========================================
// CUSTOM EVENTS - TURISTEANDO APP (ACTUALIZADO)
// ========================================

// CATEGORIAS - Turismo, Restaurantes, Comercios, Hoteles
// Basado en los eventos: turismo_clicked, restaurante_clicked, comercio_clicked, hotel_clicked
app.get('/api/analytics/categorias', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    
    // Buscar todos los eventos *_clicked
    const report = await runReport(['eventName'], ['eventCount', 'activeUsers'], { startDate, endDate }, 'eventCount', 50);
    
    // Filtrar solo los eventos de categoría
    const categoriasEvents = ['turismo_clicked', 'restaurante_clicked', 'comercio_clicked', 'hotel_clicked', 'restaurantes_clicked', 'hoteles_clicked'];
    
    const categorias = report[0].rows
      ?.filter(row => categoriasEvents.includes(row.dimensionValues[0].value))
      .map(row => {
        let nombre = row.dimensionValues[0].value.replace('_clicked', '');
        return {
          categoria: nombre.charAt(0).toUpperCase() + nombre.slice(1),
          views: parseInt(row.metricValues[0].value) || 0,
          users: parseInt(row.metricValues[1].value) || 0,
        };
      }) || [];
    
    res.json({ success: true, data: categorias });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// PUEBLOS - Vistas por pueblo
// Intenta obtener de customEvent:pueblo_nombre o screenName
app.get('/api/analytics/pueblos', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    
    // Intentar obtener pantallas que contengan "Pueblo" o nombres de pueblos conocidos
    const report = await runReport(['screenName'], ['screenPageViews', 'activeUsers'], { startDate, endDate }, 'screenPageViews', 50);
    
    // Filtrar pantallas que parezcan ser de pueblos
    const pueblos = report[0].rows
      ?.filter(row => {
        const screen = row.dimensionValues[0].value.toLowerCase();
        return screen.includes('pueblo') || 
               screen.includes('raquira') || 
               screen.includes('tinjaca') ||
               screen.includes('villa') ||
               screen.includes('mongui') ||
               screen.includes('paipa') ||
               screen.includes('tica') ||
               screen.includes('sugamuxi');
      })
      .map(row => ({
        pueblo: row.dimensionValues[0].value,
        views: parseInt(row.metricValues[0].value) || 0,
        users: parseInt(row.metricValues[1].value) || 0,
      })) || [];
    
    res.json({ success: true, data: pueblos });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// ENTIDADES - Lugares más consultados
// Combina todos los *_clicked events con sus parámetros
app.get('/api/analytics/entidades', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    
    // Intentar obtener customEvent:entity_name si existe
    let entidades = [];
    
    try {
      const report = await runReportWithFilter(
        ['customEvent:entity_name', 'eventName'],
        ['eventCount', 'activeUsers'],
        { startDate, endDate },
        {
          filter: {
            orGroup: {
              filters: [
                { fieldName: 'eventName', stringFilter: { value: 'turismo_clicked' } },
                { fieldName: 'eventName', stringFilter: { value: 'restaurante_clicked' } },
                { fieldName: 'eventName', stringFilter: { value: 'comercio_clicked' } },
                { fieldName: 'eventName', stringFilter: { value: 'hotel_clicked' } },
              ]
            }
          }
        },
        'eventCount', 30
      );
      
      entidades = report[0].rows?.map(row => ({
        entidad: row.dimensionValues[0].value,
        categoria: row.dimensionValues[1].value.replace('_clicked', ''),
        clicks: parseInt(row.metricValues[0].value) || 0,
        users: parseInt(row.metricValues[1].value) || 0,
      })) || [];
    } catch (e) {
      // Si falla, devolver datos de los eventos generales
      entidades = [];
    }
    
    res.json({ success: true, data: entidades });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// ACCIONES - Llamadas, WhatsApp, Mapas, etc.
// Basado en eventos *_action
app.get('/api/analytics/acciones', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    
    // Intentar obtener customEvent:action si existe
    let acciones = [];
    
    try {
      const report = await runReportWithFilter(
        ['customEvent:action', 'eventName'],
        ['eventCount'],
        { startDate, endDate },
        {
          filter: {
            fieldName: 'eventName',
            stringFilter: { matchType: 'CONTAINS', value: '_action' }
          }
        },
        'eventCount', 50
      );
      
      acciones = report[0].rows?.map(row => ({
        action: row.dimensionValues[0].value,
        categoria: row.dimensionValues[1].value,
        count: parseInt(row.metricValues[0].value) || 0,
      })) || [];
    } catch (e) {
      acciones = [];
    }
    
    res.json({ success: true, data: acciones });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// Resumen de acciones por tipo
app.get('/api/analytics/acciones-resumen', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    
    // Obtener todos los eventos *_action
    const report = await runReport(['eventName'], ['eventCount'], { startDate, endDate }, 'eventCount', 50);
    
    // Filtrar eventos de acción
    const accionesNombres = {
      'turismo_action': 'turismo',
      'restaurante_action': 'restaurante',
      'comercio_action': 'comercio',
      'hotel_action': 'hotel',
      'abrir_mapa': 'mapa',
      'abrir_informacion': 'informacion',
      'favorito_agregado': 'favorito',
      'filter': 'filtro',
      'button_clicked': 'boton',
      'button_click': 'boton',
    };
    
    const acciones = [];
    
    // Obtener detalles de cada acción
    report[0].rows?.forEach(row => {
      const eventName = row.dimensionValues[0].value;
      const count = parseInt(row.metricValues[0].value) || 0;
      
      if (eventName.includes('_action') || eventName.includes('abrir_') || eventName === 'favorito_agregado' || eventName === 'filter') {
        acciones.push({
          action: eventName.replace('_action', '').replace('abrir_', ''),
          count: count
        });
      }
    });
    
    // También intentar obtener customEvent:action
    try {
      const detailReport = await runReportWithFilter(
        ['customEvent:action'],
        ['eventCount'],
        { startDate, endDate },
        {
          filter: {
            fieldName: 'eventName',
            stringFilter: { matchType: 'CONTAINS', value: 'action' }
          }
        },
        'eventCount', 30
      );
      
      const customActions = detailReport[0].rows?.map(row => ({
        action: row.dimensionValues[0].value,
        count: parseInt(row.metricValues[0].value) || 0,
      })) || [];
      
      // Combinar resultados
      customActions.forEach(ca => {
        const existing = acciones.find(a => a.action === ca.action);
        if (existing) {
          existing.count += ca.count;
        } else {
          acciones.push(ca);
        }
      });
    } catch (e) {
      // Ignorar error
    }
    
    res.json({ success: true, data: acciones });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// BÚSQUEDAS
app.get('/api/analytics/busquedas', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    
    // Intentar obtener término de búsqueda
    let busquedas = [];
    
    try {
      const report = await runReportWithFilter(
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
      
      busquedas = report[0].rows?.map(row => ({
        query: row.dimensionValues[0].value,
        count: parseInt(row.metricValues[0].value) || 0,
      })) || [];
    } catch (e) {
      // Intentar con filtro
      try {
        const filterReport = await runReportWithFilter(
          ['customEvent:filter_value'],
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
        
        busquedas = filterReport[0].rows?.map(row => ({
          query: row.dimensionValues[0].value,
          count: parseInt(row.metricValues[0].value) || 0,
        })) || [];
      } catch (e2) {
        busquedas = [];
      }
    }
    
    res.json({ success: true, data: busquedas });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// ERRORES
app.get('/api/analytics/errores', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    
    // Obtener app_exception events
    const report = await runReportWithFilter(
      ['customEvent:error_type', 'customEvent:firebase_screen'],
      ['eventCount'],
      { startDate, endDate },
      {
        filter: {
          fieldName: 'eventName',
          stringFilter: { value: 'app_exception' }
        }
      },
      'eventCount', 20
    );
    
    const errores = report[0].rows?.map(row => ({
      tipo: row.dimensionValues[0].value || 'Error desconocido',
      pantalla: row.dimensionValues[1]?.value || 'N/A',
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
    
    // Intentar obtener eventos SOS
    let sos = [];
    
    try {
      const report = await runReportWithFilter(
        ['customEvent:sos_type'],
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
      
      sos = report[0].rows?.map(row => ({
        tipo: row.dimensionValues[0].value,
        count: parseInt(row.metricValues[0].value) || 0,
      })) || [];
    } catch (e) {
      sos = [];
    }
    
    res.json({ success: true, data: sos });
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
      categoriasData,
      accionesData,
      platformsData,
      countriesData,
      topEventsData
    ] = await Promise.all([
      runReport([], ['activeUsers', 'newUsers', 'sessions', 'screenPageViews', 'averageSessionDuration', 'bounceRate', 'engagementRate'], dateRange),
      runReport(['eventName'], ['eventCount', 'activeUsers'], dateRange, 'eventCount', 50),
      runReport(['eventName'], ['eventCount'], dateRange, 'eventCount', 50),
      runReport(['platform'], ['activeUsers', 'sessions'], dateRange, 'activeUsers', 5).catch(() => null),
      runReport(['country'], ['activeUsers'], dateRange, 'activeUsers', 10).catch(() => null),
      runReport(['eventName'], ['eventCount', 'activeUsers'], dateRange, 'eventCount', 30).catch(() => null)
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
    
    // Parse categorias (turismo_clicked, restaurante_clicked, etc.)
    const categoriasEvents = ['turismo_clicked', 'restaurante_clicked', 'comercio_clicked', 'hotel_clicked'];
    const categorias = categoriasData[0].rows
      ?.filter(row => categoriasEvents.includes(row.dimensionValues[0].value))
      .map(row => {
        let nombre = row.dimensionValues[0].value.replace('_clicked', '');
        return {
          categoria: nombre.charAt(0).toUpperCase() + nombre.slice(1),
          views: parseInt(row.metricValues[0].value) || 0,
          users: parseInt(row.metricValues[1].value) || 0,
        };
      }) || [];
    
    // Parse acciones
    const acciones = accionesData[0].rows
      ?.filter(row => row.dimensionValues[0].value.includes('_action') || 
                      row.dimensionValues[0].value.includes('abrir_') ||
                      row.dimensionValues[0].value === 'favorito_agregado' ||
                      row.dimensionValues[0].value === 'filter')
      .map(row => ({
        action: row.dimensionValues[0].value.replace('_action', '').replace('abrir_', ''),
        count: parseInt(row.metricValues[0].value) || 0,
      })) || [];
    
    res.json({
      success: true,
      data: {
        overview,
        categorias,
        acciones,
        platforms: platformsData?.[0]?.rows?.map(r => ({
          platform: r.dimensionValues[0].value,
          users: parseInt(r.metricValues[0].value) || 0,
          sessions: parseInt(r.metricValues[1].value) || 0
        })) || [],
        countries: countriesData?.[0]?.rows?.map(r => ({
          country: r.dimensionValues[0].value,
          users: parseInt(r.metricValues[0].value) || 0
        })) || [],
        topEvents: topEventsData?.[0]?.rows?.map(r => ({
          event: r.dimensionValues[0].value,
          count: parseInt(r.metricValues[0].value) || 0,
          users: parseInt(r.metricValues[1].value) || 0
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

