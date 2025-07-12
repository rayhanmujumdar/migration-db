import { PrismaClient as MySQLClient } from './mysql-schema/generated/mysql/index.js';
import { PrismaClient as PostgresClient } from './postgres-schema/generated/postgres/index.js';

console.log('Initializing MySQL Prisma Client');

function initializeClients() {
    const mysqlClient = new MySQLClient();
    const postgresClient = new PostgresClient();
    return { mysqlClient, postgresClient };
}

// Function to migrate company and users from MySQL to Postgres
// async function migrateCompanyOrUsers(mysqlClient, postgresClient) {
//     try {
//         const users = await mysqlClient.user.findMany();
//         console.log(`Found ${users.length} users in MySQL`);

//         for (const user of users) {
//             const userIsExistsInPostgres = await postgresClient.user.findUnique(
//                 {
//                     where: { id: user.id },
//                 }
//             );

//             if (userIsExistsInPostgres) {
//                 console.log(
//                     `User with ID ${user.id} already exists in Postgres, skipping...`
//                 );
//                 continue;
//             }
//             const company = await mysqlClient.company.findUnique({
//                 where: { id: user.companyId },
//             });

//             const companyIsExistsInPostgres =
//                 await postgresClient.company.findUnique({
//                     where: { id: company.id },
//                 });

//             if (!companyIsExistsInPostgres) {
//                 await postgresClient.company.create({
//                     data: {
//                         ...company,
//                     },
//                 });
//             }

//             await postgresClient.user.create({
//                 data: {
//                     ...user,
//                 },
//             });
//             console.log(`Migrated user with ID ${user.id} to Postgres`);
//         }
//         console.log('All users or company data migrated successfully');
//     } catch (err) {
//         console.error('Error migrating users:', err);
//         throw err;
//     }
// }

// // Function to migrate clients and leads from MySQL to Postgres
// async function migrateClientOrLead(mysqlClient, postgresClient) {
//     try {
//         const clients = await mysqlClient.client.findMany();
//         console.log(`Found ${clients.length} clients in MySQL`);

//         for (const client of clients) {
//             const clientExistsInPostgres =
//                 await postgresClient.client.findUnique({
//                     where: { id: client.id },
//                 });

//             if (clientExistsInPostgres) {
//                 console.log(
//                     `Client with ID ${client.id} already exists in Postgres, skipping...`
//                 );
//                 continue;
//             }

//             const lead =
//                 client.leadId &&
//                 (await mysqlClient.lead.findUnique({
//                     where: { id: client.leadId },
//                 }));

//             if (lead) {
//                 await postgresClient.lead.create({
//                     data: {
//                         ...lead,
//                     },
//                 });
//             }

//             await postgresClient.client.create({
//                 data: {
//                     ...client,
//                 },
//             });
//             console.log(`Migrated client with ID ${client.id} to Postgres`);
//         }
//     } catch (err) {
//         console.error('Error migrating clients:', err);
//         throw err;
//     }
// }

// // Function to migrate a specific column from MySQL to Postgres
// async function migrateColumn(mysqlClient, postgresClient) {
//     try {
//         const columnsData = await mysqlClient.column.findMany();
//         console.log(`Found ${columnsData.length} columns in MySQL`);
//         for (const column of columnsData) {
//             const columnExistsInPostgres =
//                 await postgresClient.column.findUnique({
//                     where: { id: column.id },
//                 });

//             if (columnExistsInPostgres) {
//                 console.log(
//                     `Column with ID ${column.id} already exists in Postgres, skipping...`
//                 );
//                 continue;
//             }

//             await postgresClient.column.create({
//                 data: {
//                     ...column,
//                 },
//             });
//             console.log(`Migrated column with ID ${column.id} to Postgres`);
//         }
//     } catch (err) {
//         console.error('Error migrating column:', err);
//         throw err;
//     }
// }

// // Function to migrate categories from MySQL to Postgres
// async function migrateCategory(mysqlClient, postgresClient) {
//     try {
//         const categories = await mysqlClient.category.findMany();
//         console.log(`Found ${categories.length} categories in MySQL`);

//         for (const category of categories) {
//             const categoryExistsInPostgres =
//                 await postgresClient.category.findUnique({
//                     where: { id: category.id },
//                 });

//             if (categoryExistsInPostgres) {
//                 console.log(
//                     `Category with ID ${category.id} already exists in Postgres, skipping...`
//                 );
//                 continue;
//             }

//             await postgresClient.category.create({
//                 data: {
//                     ...category,
//                 },
//             });
//             console.log(`Migrated category with ID ${category.id} to Postgres`);
//         }
//     } catch (err) {
//         console.error('Error migrating categories:', err);
//         throw err;
//     }
// }

// // Function to migrate services from MySQL to Postgres
// async function migrateService(mysqlClient, postgresClient) {
//     try {
//         const services = await mysqlClient.service.findMany();
//         console.log(`Found ${services.length} services in MySQL`);

//         for (const service of services) {
//             const serviceExistsInPostgres =
//                 await postgresClient.service.findUnique({
//                     where: { id: service.id },
//                 });

//             if (serviceExistsInPostgres) {
//                 console.log(
//                     `Service with ID ${service.id} already exists in Postgres, skipping...`
//                 );
//                 continue;
//             }

//             await postgresClient.service.create({
//                 data: {
//                     ...service,
//                 },
//             });
//             console.log(`Migrated service with ID ${service.id} to Postgres`);
//         }
//     } catch (err) {
//         console.error('Error migrating services:', err);
//         throw err;
//     }
// }

// // Function to migrate emails from MySQL to Postgres
// async function migrateTag(mysqlClient, postgresClient) {
//     try {
//         const tags = await mysqlClient.tag.findMany();
//         console.log(`Found ${tags.length} tags in MySQL`);

//         for (const tag of tags) {
//             const tagExistsInPostgres = await postgresClient.tag.findUnique({
//                 where: { id: tag.id },
//             });

//             if (tagExistsInPostgres) {
//                 console.log(
//                     `Tag with ID ${tag.id} already exists in Postgres, skipping...`
//                 );
//                 continue;
//             }

//             await postgresClient.tag.create({
//                 data: {
//                     ...tag,
//                 },
//             });
//             console.log(`Migrated tag with ID ${tag.id} to Postgres`);
//         }
//     } catch (err) {
//         console.error('Error migrating tags:', err);
//         throw err;
//     }
// }

// async function migrateVehicle(mysqlClient, postgresClient) {
//     try {
//         const vehicles = await mysqlClient.vehicle.findMany();
//         console.log(`Found ${vehicles.length} vehicles in MySQL`);

//         for (const vehicle of vehicles) {
//             const vehicleExistsInPostgres =
//                 await postgresClient.vehicle.findUnique({
//                     where: { id: vehicle.id },
//                 });

//             if (vehicleExistsInPostgres) {
//                 console.log(
//                     `Vehicle with ID ${vehicle.id} already exists in Postgres, skipping...`
//                 );
//                 continue;
//             }

//             await postgresClient.vehicle.create({
//                 data: {
//                     ...vehicle,
//                 },
//             });
//             console.log(`Migrated vehicle with ID ${vehicle.id} to Postgres`);
//         }
//     } catch (err) {
//         console.error('Error migrating vehicles:', err);
//         throw err;
//     }
// }

// dynamic model migrate function
async function migrateModel(mysqlClient, postgresClient, modelName) {
    try {
        const modelData = await mysqlClient[modelName].findMany();
        console.log(`Found ${modelData.length}  ${modelName}s in MySQL`);

        for (const vehicle of modelData) {
            const vehicleExistsInPostgres = await postgresClient[
                modelName
            ].findUnique({
                where: { id: vehicle.id },
            });

            if (vehicleExistsInPostgres) {
                console.log(
                    `${modelName} with ID ${vehicle.id} already exists in Postgres, skipping...`
                );
                continue;
            }

            await postgresClient[modelName].create({
                data: {
                    ...vehicle,
                },
            });
            console.log(
                `Migrated ${modelName} with ID ${vehicle.id} to Postgres`
            );
        }
    } catch (err) {
        console.error(`Error migrating ${modelName}:`, err);
        throw err;
    }
}

async function migrateItemTag(mysqlClient, postgresClient) {
    try {
        const itemTags = await mysqlClient.itemTag.findMany();
        console.log(`Found ${itemTags.length} item tags in MySQL`);

        for (const itemTag of itemTags) {
            const itemTagExistsInPostgres =
                await postgresClient.itemTag.findFirst({
                    where: { itemId: itemTag.itemId, tagId: itemTag.tagId },
                });

            if (itemTagExistsInPostgres) {
                console.log(
                    `Item tag with ID ${itemTag.id} already exists in Postgres, skipping...`
                );
                continue;
            }

            await postgresClient.itemTag.create({
                data: {
                    ...itemTag,
                },
            });
            console.log(`Migrated item tag with ID ${itemTag.id} to Postgres`);
        }
    } catch (err) {
        console.error('Error migrating item tags:', err);
        throw err;
    }
}

// Function to migrate MySQL data to Postgres
async function migrateMySQLToPostgres(mysqlClient, postgresClient) {
    try {
        await migrateModel(mysqlClient, postgresClient, 'company'); // 1
        await migrateModel(mysqlClient, postgresClient, 'user'); // 2
        await migrateModel(mysqlClient, postgresClient, 'category'); // 3 -> 26
        await migrateModel(mysqlClient, postgresClient, 'service'); // 4
        await migrateModel(mysqlClient, postgresClient, 'column'); // 5
        await migrateModel(mysqlClient, postgresClient, 'tag'); //6
        await migrateModel(mysqlClient, postgresClient, 'lead'); // 7
        await migrateModel(mysqlClient, postgresClient, 'client'); // 8
        await migrateModel(mysqlClient, postgresClient, 'vehicleColor'); // 9
        await migrateModel(mysqlClient, postgresClient, 'vehicle'); // 10
        await migrateModel(mysqlClient, postgresClient, 'fleet'); // 11
        await migrateModel(mysqlClient, postgresClient, 'fleetStatement'); // 12

        await migrateModel(mysqlClient, postgresClient, 'invoice'); // 13
        await migrateModel(mysqlClient, postgresClient, 'appointment'); // 14
        await migrateModel(mysqlClient, postgresClient, 'appointmentUser'); // 15
        await migrateModel(mysqlClient, postgresClient, 'attachment'); // 16
        await migrateModel(mysqlClient, postgresClient, 'calendarSettings'); // 17
        await migrateModel(mysqlClient, postgresClient, 'payment'); // 18
        await migrateModel(mysqlClient, postgresClient, 'paymentMethod'); // 19
        await migrateModel(mysqlClient, postgresClient, 'refund'); // 20
        await migrateModel(mysqlClient, postgresClient, 'cardPayment'); // 21
        await migrateModel(mysqlClient, postgresClient, 'cashPayment'); // 22
        await migrateModel(mysqlClient, postgresClient, 'checkPayment'); // 23
        await migrateModel(mysqlClient, postgresClient, 'depositPayment'); // 24
        await migrateModel(mysqlClient, postgresClient, 'otherPayment'); // 25
        await migrateModel(mysqlClient, postgresClient, 'stripePayment'); // 26
        await migrateModel(mysqlClient, postgresClient, 'clientCall'); // 27
        await migrateModel(
            mysqlClient,
            postgresClient,
            'clientConversationTrack'
        ); // 28
        await migrateModel(mysqlClient, postgresClient, 'clientCoupon'); // 29
        await migrateModel(mysqlClient, postgresClient, 'clientSMS'); // 30
        await migrateModel(mysqlClient, postgresClient, 'clientSmsAttachments'); // 31
        await migrateModel(mysqlClient, postgresClient, 'clockInOut'); // 32
        await migrateModel(mysqlClient, postgresClient, 'clockBreak'); // 33
        await migrateModel(
            mysqlClient,
            postgresClient,
            'communicationAutomationRule'
        ); // 34
        await migrateModel(mysqlClient, postgresClient, 'communicationStage'); // 35
        await migrateModel(mysqlClient, postgresClient, 'companyEmailTemplate'); // 36
        await migrateModel(mysqlClient, postgresClient, 'companyJoin'); // 37
        await migrateModel(mysqlClient, postgresClient, 'coupon'); // 38
        await migrateModel(mysqlClient, postgresClient, 'emailTemplate'); // 39
        await migrateModel(mysqlClient, postgresClient, 'group'); // 40
        await migrateModel(mysqlClient, postgresClient, 'holiday'); // 41
        await migrateModel(mysqlClient, postgresClient, 'vendor'); // 42
        await migrateModel(mysqlClient, postgresClient, 'inventoryProduct'); // 43
        await migrateModel(
            mysqlClient,
            postgresClient,
            'inventoryProductHistory'
        ); // 44
        await migrateModel(mysqlClient, postgresClient, 'inventoryProductTag'); // 45
        // await migrateModel(
        //     mysqlClient,
        //     postgresClient,
        //     'inventoryWirehouseProduct'
        // ); // 46
        await migrateModel(
            mysqlClient,
            postgresClient,
            'invoiceAutomationRule'
        ); // 47
        await migrateModel(mysqlClient, postgresClient, 'technician'); // 52  //TODO:
        await migrateModel(mysqlClient, postgresClient, 'invoiceInspection'); // 48
        await migrateModel(mysqlClient, postgresClient, 'labor'); // 49
        await migrateModel(mysqlClient, postgresClient, 'invoiceItem'); // 50
        await migrateModel(mysqlClient, postgresClient, 'invoicePhoto'); // 51
        await migrateModel(mysqlClient, postgresClient, 'invoiceRedo'); // 53
        await migrateModel(mysqlClient, postgresClient, 'invoiceTags'); // 54
        await migrateItemTag(mysqlClient, postgresClient); // 55
        await migrateModel(mysqlClient, postgresClient, 'laborTag'); // 56
        await migrateModel(mysqlClient, postgresClient, 'leadLink'); // 57
        await migrateModel(mysqlClient, postgresClient, 'leadTags'); // 58
        await migrateModel(mysqlClient, postgresClient, 'leaveRequest'); // 59
        await migrateModel(mysqlClient, postgresClient, 'mailgunCredential'); // 60
        await migrateModel(mysqlClient, postgresClient, 'mailgunEmail'); // 61
        await migrateModel(
            mysqlClient,
            postgresClient,
            'mailgunEmailAttachment'
        ); // 62
        await migrateModel(
            mysqlClient,
            postgresClient,
            'marketingAutomationRule'
        ); // 63
        await migrateModel(mysqlClient, postgresClient, 'material'); // 64
        await migrateModel(mysqlClient, postgresClient, 'materialTag'); // 65
        await migrateModel(mysqlClient, postgresClient, 'message'); // 66
        await migrateModel(mysqlClient, postgresClient, 'chatTrack'); // 90
        await migrateModel(
            mysqlClient,
            postgresClient,
            'notificationSettingsV2'
        ); // 67
        await migrateModel(mysqlClient, postgresClient, 'oAuthToken'); // 68
        await migrateModel(mysqlClient, postgresClient, 'passwordResetToken'); // 69
        await migrateModel(mysqlClient, postgresClient, 'permission'); // 70
        await migrateModel(mysqlClient, postgresClient, 'permissionForManager'); // 71
        await migrateModel(mysqlClient, postgresClient, 'permissionForOther'); // 72
        await migrateModel(mysqlClient, postgresClient, 'permissionForSales'); // 73
        await migrateModel(
            mysqlClient,
            postgresClient,
            'permissionForTechnician'
        ); // 74
        await migrateModel(
            mysqlClient,
            postgresClient,
            'pipelineAutomationRule'
        ); // 75
        await migrateModel(mysqlClient, postgresClient, 'pipelineStage'); // 76
        await migrateModel(mysqlClient, postgresClient, 'requestEstimate'); // 77
        await migrateModel(
            mysqlClient,
            postgresClient,
            'serviceMaintenanceAutomationRule'
        ); // 78
        await migrateModel(
            mysqlClient,
            postgresClient,
            'serviceMaintenanceStage'
        ); // 79
        await migrateModel(mysqlClient, postgresClient, 'source'); // 80
        await migrateModel(mysqlClient, postgresClient, 'status'); // 81
        await migrateModel(mysqlClient, postgresClient, 'task'); // 82
        await migrateModel(mysqlClient, postgresClient, 'taskUser'); // 83
        await migrateModel(mysqlClient, postgresClient, 'timeDelayExecution'); // 84
        await migrateModel(mysqlClient, postgresClient, 'twilioCredentials'); // 85
        await migrateModel(mysqlClient, postgresClient, 'userFeedback'); // 86
        await migrateModel(
            mysqlClient,
            postgresClient,
            'userFeedbackAttachment'
        ); // 87
        await migrateModel(mysqlClient, postgresClient, 'vehicleParts'); // 88
        await migrateModel(mysqlClient, postgresClient, 'automationAttachment'); // 89
        console.log('Migration from MySQL to Postgres completed successfully');
    } catch (err) {
        console.error('Error during migration:', err);
        throw err;
    }
}

async function main() {
    try {
        const { mysqlClient, postgresClient } = initializeClients();

        // Test the database connection first
        console.log('Testing database connection...');

        // Test MySQL connection
        try {
            await mysqlClient.$connect();
            await postgresClient.$connect();
            console.log('MySQL connection successful');
        } catch (err) {
            console.error('MySQL connection failed:', err);
            return;
        }

        // Check if User model exists by trying to count records
        try {
            await migrateMySQLToPostgres(mysqlClient, postgresClient);
        } catch (err) {
            console.error('Error querying users from MySQL:', err);
        }

        // Close the client when done
        await mysqlClient.$disconnect();
        await postgresClient.$disconnect();
    } catch (err) {
        console.error('Error initializing Prisma client:', err);
        process.exit(1);
    }
}

main();
