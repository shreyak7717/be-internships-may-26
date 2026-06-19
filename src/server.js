import Fastify from 'fastify';
import dotenv from 'dotenv';
import { postSignal, getSignals } from './signals.js';

dotenv.config();
const API_KEY = process.env.API_KEY || 'change-me';
const PORT = Number(process.env.PORT || 8080);

const app = Fastify({ logger: { level: 'info' } });

app.addHook('onRequest', async (req, reply) => {
  if (req.routerPath === '/healthz') return;
  const key = req.headers['x-api-key'];
  if (!key || key !== API_KEY) {
    return reply.code(401).send({ error: 'unauthorized' });
  }
});

app.get('/healthz', async () => ({ ok: true }));
app.post('/v1/signals', postSignal);
app.get('/v1/signals', getSignals);

const start = async () => {
  try {
    await app.listen({ host: '0.0.0.0', port: PORT });
  } catch (e) {
    app.log.error(e);
    process.exit(1);
  }
};

const shutdown = async (signal) => {
  app.log.info({ signal }, 'Shutting down gracefully...');
  try {
    await app.close();
    app.log.info('Server closed. Exiting.');
    process.exit(0);
  } catch (e) {
    app.log.error({ err: e }, 'Error during shutdown');
    process.exit(1);
  }
};

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

start();