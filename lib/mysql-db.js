import { PrismaClient } from '@prisma/client';

const globalForPrisma = globalThis;
const extendClient = new PrismaClient();
export const mysql_db = globalForPrisma.prisma || extendClient;

if (process.env.NODE_ENV !== 'production') {
    globalForPrisma.prisma = db;
}
