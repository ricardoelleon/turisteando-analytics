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

const PROPERTY_ID = '487082948';

app.get('/api/health', (req, res) => {
  res.json({ status: 'OK', timestamp: new Date().toISOString() });
});

app.get('/api/analytics/overview', async (req, res) => {
  try {
    const { startDate = '7daysAgo', endDate = 'today', platform } = req.query;
    
    const dimensionFilters = [];
    if (platform && platform !== 'all') {
      dimensionFilters.push({
        name: 'platform',
        stringFilter: { value: platform }
      });
    }

    const [response] = await analyticsDataClient.runReport({
      property: `properties/${PROPERTY_ID}`,
      dateRanges: [{ startDate, endDate }],
      metrics: [
        { name: 'activeUsers' },
        { name: 'newUsers' },
        { name: 'sessions' },
        { name: 'screenPageViews' },
        { name: 'averageSessionDuration' },
        { name: 'bounceRate' },
        { name: 'engagementRate' }
      ],
      dimensionFilters: dimensionFilters.length > 0 ? [{ filter: dimensionFilters[0] }] : undefined,
    });

    const data = {
      activeUsers: 0, newUsers: 0, sessions: 0, pageViews: 0,
      avgSessionDuration: 0, bounceRate: 0, engagementRate: 0
    };

    if (response.rows && response.rows.length > 0) {
      const row = response.rows[0];
      data.activeUsers = parseInt(row.metricValues[0].value) || 0;
      data.newUsers = parseInt(row.metricValues[1].value) || 0;
      data.sessions = parseInt(row.metricValues[2].value) || 0;
      data.pageViews = parseInt(row.metricValues[3].value) || 0;
      data.avgSessionDuration = parseFloat(row.metricValues[4].value) || 0;
      data.bounceRate = parseFloat(row.metricValues[5].value) || 0;
      data.engagementRate = parseFloat(row.metricValues[6].value) || 0;
    }

    res.json({ success: true, data });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.get('/api/analytics/users-by-day', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today', platform } = req.query;
    const dimensionFilters = [];
    if (platform && platform !== 'all') {
      dimensionFilters.push({ name: 'platform', stringFilter: { value: platform } });
    }

    const [response] = await analyticsDataClient.runReport({
      property: `properties/${PROPERTY_ID}`,
      dateRanges: [{ startDate, endDate }],
      dimensions: [{ name: 'date' }],
      metrics: [{ name: 'activeUsers' }, { name: 'newUsers' }, { name: 'sessions' }],
      dimensionFilters: dimensionFilters.length > 0 ? [{ filter: dimensionFilters[0] }] : undefined,
      orderBys: [{ dimension: { dimensionName: 'date' }, desc: false }],
    });

    const data = (response.rows || []).map(row => ({
      date: row.dimensionValues[0].value,
      activeUsers: parseInt(row.metricValues[0].value) || 0,
      newUsers: parseInt(row.metricValues[1].value) || 0,
      sessions: parseInt(row.metricValues[2].value) || 0
    }));

    res.json({ success: true, data });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.get('/api/analytics/by-platform', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today' } = req.query;
    const [response] = await analyticsDataClient.runReport({
      property: `properties/${PROPERTY_ID}`,
      dateRanges: [{ startDate, endDate }],
      dimensions: [{ name: 'platform' }],
      metrics: [{ name: 'activeUsers' }, { name: 'newUsers' }, { name: 'sessions' }, { name: 'screenPageViews' }],
    });

    const data = (response.rows || []).map(row => ({
      platform: row.dimensionValues[0].value,
      activeUsers: parseInt(row.metricValues[0].value) || 0,
      newUsers: parseInt(row.metricValues[1].value) || 0,
      sessions: parseInt(row.metricValues[2].value) || 0,
      pageViews: parseInt(row.metricValues[3].value) || 0
    }));

    res.json({ success: true, data });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.get('/api/analytics/top-screens', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today', platform } = req.query;
    const limit = parseInt(req.query.limit) || 10;
    const dimensionFilters = [];
    if (platform && platform !== 'all') {
      dimensionFilters.push({ name: 'platform', stringFilter: { value: platform } });
    }

    const [response] = await analyticsDataClient.runReport({
      property: `properties/${PROPERTY_ID}`,
      dateRanges: [{ startDate, endDate }],
      dimensions: [{ name: 'screenName' }],
      metrics: [{ name: 'screenPageViews' }, { name: 'activeUsers' }],
      dimensionFilters: dimensionFilters.length > 0 ? [{ filter: dimensionFilters[0] }] : undefined,
      orderBys: [{ metric: { metricName: 'screenPageViews' }, desc: true }],
      limit,
    });

    const data = (response.rows || []).map(row => ({
      screen: row.dimensionValues[0].value,
      views: parseInt(row.metricValues[0].value) || 0,
      users: parseInt(row.metricValues[1].value) || 0
    }));

    res.json({ success: true, data });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.get('/api/analytics/top-events', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today', platform } = req.query;
    const limit = parseInt(req.query.limit) || 15;
    const dimensionFilters = [];
    if (platform && platform !== 'all') {
      dimensionFilters.push({ name: 'platform', stringFilter: { value: platform } });
    }

    const [response] = await analyticsDataClient.runReport({
      property: `properties/${PROPERTY_ID}`,
      dateRanges: [{ startDate, endDate }],
      dimensions: [{ name: 'eventName' }],
      metrics: [{ name: 'eventCount' }, { name: 'activeUsers' }],
      dimensionFilters: dimensionFilters.length > 0 ? [{ filter: dimensionFilters[0] }] : undefined,
      orderBys: [{ metric: { metricName: 'eventCount' }, desc: true }],
      limit,
    });

    const data = (response.rows || []).map(row => ({
      event: row.dimensionValues[0].value,
      count: parseInt(row.metricValues[0].value) || 0,
      users: parseInt(row.metricValues[1].value) || 0
    }));

    res.json({ success: true, data });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.get('/api/analytics/by-country', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today', platform } = req.query;
    const limit = parseInt(req.query.limit) || 10;
    const dimensionFilters = [];
    if (platform && platform !== 'all') {
      dimensionFilters.push({ name: 'platform', stringFilter: { value: platform } });
    }

    const [response] = await analyticsDataClient.runReport({
      property: `properties/${PROPERTY_ID}`,
      dateRanges: [{ startDate, endDate }],
      dimensions: [{ name: 'country' }],
      metrics: [{ name: 'activeUsers' }, { name: 'newUsers' }],
      dimensionFilters: dimensionFilters.length > 0 ? [{ filter: dimensionFilters[0] }] : undefined,
      orderBys: [{ metric: { metricName: 'activeUsers' }, desc: true }],
      limit,
    });

    const data = (response.rows || []).map(row => ({
      country: row.dimensionValues[0].value,
      users: parseInt(row.metricValues[0].value) || 0,
      newUsers: parseInt(row.metricValues[1].value) || 0
    }));

    res.json({ success: true, data });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.get('/api/analytics/acquisition', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today', platform } = req.query;
    const dimensionFilters = [];
    if (platform && platform !== 'all') {
      dimensionFilters.push({ name: 'platform', stringFilter: { value: platform } });
    }

    const [response] = await analyticsDataClient.runReport({
      property: `properties/${PROPERTY_ID}`,
      dateRanges: [{ startDate, endDate }],
      dimensions: [{ name: 'firstUserSource' }],
      metrics: [{ name: 'newUsers' }, { name: 'sessions' }, { name: 'engagementRate' }],
      dimensionFilters: dimensionFilters.length > 0 ? [{ filter: dimensionFilters[0] }] : undefined,
      orderBys: [{ metric: { metricName: 'newUsers' }, desc: true }],
      limit: 10,
    });

    const data = (response.rows || []).map(row => ({
      source: row.dimensionValues[0].value || 'Direct',
      newUsers: parseInt(row.metricValues[0].value) || 0,
      sessions: parseInt(row.metricValues[1].value) || 0,
      engagementRate: parseFloat(row.metricValues[2].value) || 0
    }));

    res.json({ success: true, data });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.get('/api/analytics/retention', async (req, res) => {
  try {
    const { startDate = '30daysAgo', endDate = 'today', platform } = req.query;
    const dimensionFilters = [];
    if (platform && platform !== 'all') {
      dimensionFilters.push({ name: 'platform', stringFilter: { value: platform } });
    }

    const [response] = await analyticsDataClient.runReport({
      property: `properties/${PROPERTY_ID}`,
      dateRanges: [{ startDate, endDate }],
      dimensions: [{ name: 'dayOfWeek' }],
      metrics: [{ name: 'activeUsers' }, { name: 'returningUsers' }],
      dimensionFilters: dimensionFilters.length > 0 ? [{ filter: dimensionFilters[0] }] : undefined,
    });

    const dayNames = ['Domingo', 'Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado'];
    const data = (response.rows || []).map(row => ({
      day: dayNames[parseInt(row.dimensionValues[0].value)] || row.dimensionValues[0].value,
      activeUsers: parseInt(row.metricValues[0].value) || 0,
      returningUsers: parseInt(row.metricValues[1].value) || 0
    }));

    res.json({ success: true, data });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.listen(PORT, () => {
  console.log(`🚀 Analytics Server running on port ${PORT}`);
});
