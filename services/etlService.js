const EtlLogger = require('../models/EtlLogger');
const logger = require('../utils/logger');
const { sourcePool, targetPool } = require('../config/db');

class EtlService {
    constructor(sourcePool, targetPool) {
        this.sourcePool = sourcePool;
        this.targetPool = targetPool;
        this.etlLogger = new EtlLogger(sourcePool);
        this.ONE_TO_ONE_TABLES = [
            'farmers_kyc1', 'farmers_kyc2', 'farmers_kyc3', 'farmers_kyc4'
        ];
        this.lastRun = null;
        this.batchSize = 50000;
        this.concurrencyLimit = 4;
    }

    async getSourceData(table, rsbsaNos) {
        if (rsbsaNos.length === 0) return [];
        if (table === 'farmparcel') {
            const [ownerships] = await this.sourcePool.query(
                `SELECT parcel_id, rsbsa_no FROM farmparcelownership WHERE rsbsa_no IN (?)`,
                [rsbsaNos]
            );
            if (ownerships.length === 0) return [];

            const parcelIds = ownerships.map(o => o.parcel_id);
            const [rows] = await this.sourcePool.query(
                `SELECT * FROM farmparcel WHERE parcel_id IN (?)`,
                [parcelIds]
            );
            return rows;
        } else {
            const [rows] = await this.sourcePool.query(
                `SELECT * FROM ${table} WHERE rsbsa_no IN (?)`,
                [rsbsaNos]
            );
            return rows;
        }
    }

    async transferData(table, data) {
        await this.ensureTableExists(table);
        if (data.length === 0) return;

        const processedRecords = data.map(record => this.processDataForTable(table, record));
        if (this.ONE_TO_ONE_TABLES.includes(table)) {
            await this.handleOneToOneTransfer(table, processedRecords);
        } else {
            await this.handleOneToManyTransfer(table, processedRecords);
        }
    }

    async handleOneToOneTransfer(table, records) {
        if (records.length === 0) return;
        const connection = await this.targetPool.getConnection();
        try {
            await connection.beginTransaction();
            const rsbsaNos = records.map(r => r.rsbsa_no);
            const [existing] = await connection.query(
                `SELECT rsbsa_no FROM ${table} WHERE rsbsa_no IN (?)`,
                [rsbsaNos]
            );
            const existingRsbsaNos = new Set(existing.map(r => r.rsbsa_no));

            const updates = records.filter(r => existingRsbsaNos.has(r.rsbsa_no));
            const inserts = records.filter(r => !existingRsbsaNos.has(r.rsbsa_no));

            if (updates.length > 0) {
                await this.bulkUpdateRecords(table, updates, connection);
            }
            if (inserts.length > 0) {
                await this.bulkInsertRecords(table, inserts, connection);
            }

            await connection.commit();
        } catch (error) {
            await connection.rollback();
            throw error;
        } finally {
            connection.release();
        }
    }

    async handleOneToManyTransfer(table, records) {
        if (records.length === 0) return;
        const connection = await this.targetPool.getConnection();
        try {
            await connection.beginTransaction();
            if (table === 'farmparcel') {
                const parcelIds = records.map(r => r.parcel_id);
                if (parcelIds.length > 0) {
                    await connection.query(
                        `DELETE FROM ${table} WHERE parcel_id IN (?)`,
                        [parcelIds]
                    );
                }
            } else {
                const rsbsaNos = [...new Set(records.map(r => r.rsbsa_no))];
                if (rsbsaNos.length > 0) {
                    await connection.query(
                        `DELETE FROM ${table} WHERE rsbsa_no IN (?)`,
                        [rsbsaNos]
                    );
                }
            }
            await this.bulkInsertRecords(table, records, connection);
            await connection.commit();
        } catch (error) {
            await connection.rollback();
            throw error;
        } finally {
            connection.release();
        }
    }

    async bulkUpdateRecords(table, records, connection) {
        if (records.length === 0) return;
        const columns = Object.keys(records[0]).filter(col => col !== 'rsbsa_no');
        const setClause = columns.map(col => `${col} = VALUES(${col})`).join(', ');
        const placeholders = records.map(() => `(${columns.map(() => '?').join(', ')}, ?)`).join(', ');
        const values = records.flatMap(data => [...columns.map(col => data[col]), data.rsbsa_no]);

        await connection.query(
            `INSERT INTO ${table} (${columns.join(', ')}, rsbsa_no)
             VALUES ${placeholders}
             ON DUPLICATE KEY UPDATE ${setClause}`,
            values
        );
    }

    async bulkInsertRecords(table, records, connection) {
        if (records.length === 0) return;
        const columns = Object.keys(records[0]);
        const escapedColumns = columns.map(col => {
            const reservedKeywords = ['long', 'group', 'order', 'desc', 'primary'];
            return reservedKeywords.includes(col.toLowerCase()) ? `\`${col}\`` : col;
        });
        const placeholders = records.map(() => `(${columns.map(() => '?').join(', ')})`).join(', ');
        const values = records.flatMap(data => columns.map(col => data[col]));

        await connection.query(
            `INSERT INTO ${table} (${escapedColumns.join(', ')}) VALUES ${placeholders}`,
            values
        );
    }

    processDataForTable(table, data) {
        switch (table) {
            case 'farmers_kyc1':
                return this.uppercaseFields(data, [
                    'data_source', 'first_name', 'middle_name', 'surname', 'ext_name',
                    'mother_maiden_name', 'maiden_fname', 'maiden_mname', 'maiden_lname',
                    'maiden_extname', 'birth_prv', 'birth_prv_mun', 'street'
                ]);
            case 'farmers_kyc2':
                return this.uppercaseFields(data, [
                    'mob_number_fname', 'mob_number_mname', 'mob_number_lname',
                    'mob_number_extname', 'spouse', 'hh_head_name', 'hh_relationship',
                    'emergency_name'
                ]);
            case 'farmers_kyc3':
                return this.uppercaseFields(data, [
                    'vtc_bgy_chair', 'vtc_agri_office', 'vtc_mafc_chair'
                ]);
            case 'farmers_kyc4':
                return this.uppercaseFields(data, [
                    'encoder_fullname', 'encoder_fullname_updated', 'deceased_reason'
                ]);
            case 'farmers_attachments':
            case 'farmers_fca':
            case 'farmers_form_attachments':
                return this.uppercaseFields(data, ['encoder_fullname']);
            case 'farmers_livelihood':
                return this.uppercaseFields(data, [
                    'livelihood', 'activity_work', 'specify'
                ]);
            case 'farmparcelactivity':
            case 'farmparcelattachments':
            case 'farmparcel':
                return this.uppercaseFields(data, [
                    'owner_firstname', 'owner_lastname', 'owner_extname',
                    'farmers_rotation_fullname', 'desc_location', 'unit_measure',
                    'own_doc_no', 'attachment'
                ]);
            case 'farmparcelownership':
                return this.uppercaseFields(data, ['encoder_agency', 'encoder_fullname']);
            default:
                return data;
        }
    }

    uppercaseFields(data, fields) {
        const processed = { ...data };
        fields.forEach(field => {
            if (processed[field] && typeof processed[field] === 'string') {
                processed[field] = processed[field].toUpperCase();
            }
        });
        return processed;
    }

    async ensureTableExists(table) {
        switch (table) {
            case 'farmers_kyc1':
                await this.createFarmersKyc1();
                break;
            case 'farmers_kyc2':
                await this.createFarmersKyc2();
                break;
            case 'farmers_kyc3':
                await this.createFarmersKyc3();
                break;
            case 'farmers_kyc4':
                await this.createFarmersKyc4();
                break;
            case 'farmers_attachments':
                await this.createFarmersAttachments();
                break;
            case 'farmers_fca':
                await this.createFarmersFca();
                break;
            case 'farmers_form_attachments':
                await this.createFarmersFormAttachments();
                break;
            case 'farmers_livelihood':
                await this.createFarmersLivelihood();
                break;
            case 'farmparcelactivity':
                await this.createFarmParcelActivity();
                break;
            case 'farmparcelattachments':
                await this.createFarmParcelAttachments();
                break;
            case 'farmparcel':
                await this.createFarmParcel();
                break;
            case 'farmparcelownership':
                await this.createFarmParcelOwnership();
                break;
        }
    }

    async createFarmersKyc1() {
        const sql = `
            CREATE TABLE IF NOT EXISTS farmers_kyc1 (
                kyc1_id INT(11) AUTO_INCREMENT PRIMARY KEY,
                farmerID VARCHAR(50),
                philsys_trn VARCHAR(50),
                philsys_pcn VARCHAR(50),
                sequence INT(11),
                rsbsa_no VARCHAR(50),
                source_rsbsa_no VARCHAR(50),
                data_source ENUM('FFRS', 'NFFIS', 'NCFRSS', 'NIA', 'FISHR'),
                other_sys_gen_id VARCHAR(100),
                other_sys_id VARCHAR(100),
                enrollment VARCHAR(1),
                file_picture VARCHAR(100),
                control_no VARCHAR(50),
                first_name VARCHAR(120),
                middle_name VARCHAR(100),
                surname VARCHAR(100),
                ext_name VARCHAR(50),
                mother_maiden_name VARCHAR(220),
                spouse_rsbsa_no VARCHAR(50),
                maiden_fname VARCHAR(50),
                maiden_mname VARCHAR(50),
                maiden_lname VARCHAR(50),
                maiden_extname VARCHAR(50),
                sex TINYINT(1),
                birthday DATE,
                birth_place VARCHAR(50),
                birth_prv VARCHAR(200),
                birth_prv_mun VARCHAR(100),
                house_no VARCHAR(255),
                street VARCHAR(255),
                brgy1 TINYINT(3) UNSIGNED ZEROFILL,
                mun1 TINYINT(2) UNSIGNED ZEROFILL,
                prv1 TINYINT(2) UNSIGNED ZEROFILL,
                reg1 TINYINT(2) UNSIGNED ZEROFILL,
                geo_code VARCHAR(9),
                geocode VARCHAR(15),
                brgy INT(3) UNSIGNED ZEROFILL,
                mun INT(2) UNSIGNED ZEROFILL,
                prv INT(3) UNSIGNED ZEROFILL,
                reg INT(2) UNSIGNED ZEROFILL,
                ncr_brgy INT(3) UNSIGNED ZEROFILL,
                ncr_mun INT(2) UNSIGNED ZEROFILL,
                ncr_prv INT(3) UNSIGNED ZEROFILL,
                ncr_reg INT(2) UNSIGNED ZEROFILL,
                ncr_house_no VARCHAR(255),
                ncr_street VARCHAR(255),
                c_date DATETIME,
                clone_by_id VARCHAR(50),
                clone_by_fullname VARCHAR(120),
                date_cloned TIMESTAMP,
                v1_v2 TINYINT(1),
                INDEX idx_rsbsa_no (rsbsa_no)
            )`;
        await this.targetPool.query(sql);
    }

    async createFarmersKyc2() {
        const sql = `
            CREATE TABLE IF NOT EXISTS farmers_kyc2 (
                kyc2_id INT(11) AUTO_INCREMENT PRIMARY KEY,
                rsbsa_no VARCHAR(50),
                contact_num VARCHAR(20),
                contact_num_question TINYINT(1),
                mob_number_fname VARCHAR(50),
                mob_number_mname VARCHAR(50),
                mob_number_lname VARCHAR(50),
                mob_number_extname VARCHAR(50),
                landline_num VARCHAR(20),
                education TINYINT(1),
                pwd TINYINT(1),
                religion VARCHAR(50),
                civil_status TINYINT(1),
                spouse VARCHAR(220),
                spouse_fname VARCHAR(50),
                spouse_mname VARCHAR(50),
                spouse_lname VARCHAR(50),
                spouse_extname VARCHAR(50),
                spouse_rsbsa_no VARCHAR(50),
                beneficiary_4ps TINYINT(1),
                ind_ans TINYINT(1),
                ind_id VARCHAR(50),
                gov_ans TINYINT(1),
                gov_id VARCHAR(50),
                gov_id_num VARCHAR(50),
                hh_head TINYINT(1),
                hh_head_name VARCHAR(255),
                hh_relationship VARCHAR(50),
                hh_no_members INT(11),
                hh_no_male INT(11),
                hh_no_female INT(11),
                fca_ans TINYINT(1),
                fca_id VARCHAR(50),
                emergency_name VARCHAR(220),
                emergency_contact VARCHAR(50),
                INDEX idx_rsbsa_no (rsbsa_no)
            )`;
        await this.targetPool.query(sql);
    }

    async createFarmersKyc3() {
        const sql = `
            CREATE TABLE IF NOT EXISTS farmers_kyc3 (
                kyc3_id INT(11) AUTO_INCREMENT PRIMARY KEY,
                rsbsa_no VARCHAR(50),
                no_farm_parcels INT(11),
                arb TINYINT(1),
                gross_income_farming DECIMAL(10,2),
                gross_income_nonfarming DECIMAL(10,2),
                vtc_date DATE,
                vtc_bgy_chair VARCHAR(255),
                vtc_agri_office VARCHAR(150),
                vtc_mafc_chair VARCHAR(255),
                INDEX idx_rsbsa_no (rsbsa_no)
            )`;
        await this.targetPool.query(sql);
    }

    async createFarmersKyc4() {
        const sql = `
            CREATE TABLE IF NOT EXISTS farmers_kyc4 (
                kyc4_id INT(11) AUTO_INCREMENT PRIMARY KEY,
                rsbsa_no VARCHAR(50),
                encoder_agency VARCHAR(50),
                encoder_id VARCHAR(50),
                encoder_fullname VARCHAR(120),
                encoder_id_updated VARCHAR(50),
                encoder_fullname_updated VARCHAR(120),
                date_created TIMESTAMP,
                date_updated TIMESTAMP,
                deceased ENUM('1','0'),
                deceased_reason TINYTEXT,
                ch_occupation ENUM('active','inactive'),
                ch_occupation_reason TINYTEXT,
                duplicated ENUM('1','0'),
                duplicated_reason TINYTEXT,
                duplicated_rsbsa_no TINYTEXT,
                rffa2_cashout TINYINT(4),
                validated ENUM('1','0','2'),
                unvalidated_reason TINYTEXT,
                validator_by_id VARCHAR(100),
                validator_fullname VARCHAR(100),
                date_validated DATETIME,
                submitted ENUM('1','0'),
                date_submitted DATETIME,
                submitted_by_id VARCHAR(100),
                submitted_by_fullname VARCHAR(100),
                rfo_validated ENUM('1','0'),
                rfo_date_validated DATETIME,
                rfo_validated_id VARCHAR(100),
                rfo_validated_fullname VARCHAR(100),
                online_applicant ENUM('1','0'),
                checked_date DATETIME,
                checked ENUM('1','0'),
                checked_by_id VARCHAR(50),
                checked_fullname VARCHAR(100),
                complete_cloned_by_fullname VARCHAR(120),
                complete_cloned_by_id VARCHAR(50),
                date_cloned_completed TIMESTAMP,
                rsbsa_liveness_verified INT(1),
                rsbsa_last_liveness_date DATETIME,
                rsbsa_last_user_id_liveness VARCHAR(50),
                rsbsa_last_user_fullname_liveness VARCHAR(100),
                philsys_liveness_verified INT(1),
                philsys_last_liveness_date DATETIME,
                philsys_last_user_id_liveness VARCHAR(50),
                philsys_last_user_fullname_liveness VARCHAR(100),
                INDEX idx_rsbsa_no (rsbsa_no)
            )`;
        await this.targetPool.query(sql);
    }

    async createFarmersAttachments() {
        const sql = `
            CREATE TABLE IF NOT EXISTS farmers_attachments (
                fatt_id INT(11) AUTO_INCREMENT PRIMARY KEY,
                rsbsa_no VARCHAR(50),
                filename VARCHAR(200),
                validity_file ENUM('1','0','2'),
                date_created TIMESTAMP,
                active ENUM('1','0'),
                encoder_agency VARCHAR(50),
                encoder_id VARCHAR(50),
                encoder_fullname VARCHAR(255),
                INDEX idx_rsbsa_no (rsbsa_no)
            )`;
        await this.targetPool.query(sql);
    }

    async createFarmersFca() {
        const sql = `
            CREATE TABLE IF NOT EXISTS farmers_fca (
                id INT(11) AUTO_INCREMENT PRIMARY KEY,
                rsbsa_no VARCHAR(50),
                fca_id VARCHAR(50),
                fca_name VARCHAR(255),
                date_created TIMESTAMP,
                active ENUM('1','0'),
                encoder_agency VARCHAR(50),
                encoder_id VARCHAR(50),
                encoder_fullname VARCHAR(255),
                INDEX idx_rsbsa_no (rsbsa_no)
            )`;
        await this.targetPool.query(sql);
    }

    async createFarmersFormAttachments() {
        const sql = `
            CREATE TABLE IF NOT EXISTS farmers_form_attachments (
                ffatt_id INT(11) AUTO_INCREMENT PRIMARY KEY,
                rsbsa_no VARCHAR(50),
                filename VARCHAR(200),
                date_created TIMESTAMP,
                active ENUM('1','0'),
                encoder_agency VARCHAR(50),
                encoder_id VARCHAR(50),
                encoder_fullname VARCHAR(255),
                INDEX idx_rsbsa_no (rsbsa_no)
            )`;
        await this.targetPool.query(sql);
    }

    async createFarmersLivelihood() {
        const sql = `
            CREATE TABLE IF NOT EXISTS farmers_livelihood (
                farmlivelihoodID INT(11) AUTO_INCREMENT PRIMARY KEY,
                rsbsa_no VARCHAR(50),
                livelihood VARCHAR(100),
                activity_work VARCHAR(150),
                specify VARCHAR(255),
                active ENUM('1','0'),
                INDEX idx_rsbsa_no (rsbsa_no)
            )`;
        await this.targetPool.query(sql);
    }

    async createFarmParcelActivity() {
        const sql = `
            CREATE TABLE IF NOT EXISTS farmparcelactivity (
                farmlanddetailsID INT(11) AUTO_INCREMENT PRIMARY KEY,
                parcel_id VARCHAR(50),
                rsbsa_no VARCHAR(50),
                crop_id INT(11),
                size DECIMAL(10,4),
                temp_size DECIMAL(10,4),
                orig DECIMAL(10,4),
                no_heads INT(11),
                farm_type TINYINT(1),
                organic TINYINT(1),
                active ENUM('1','0'),
                encoder_agency VARCHAR(50),
                encoder_id VARCHAR(50),
                encoder_fullname VARCHAR(255),
                date_created TIMESTAMP,
                slip_b_update TINYINT(4),
                from_slip_b_update TINYINT(4),
                intercrop ENUM('1','2'),
                crop_date_start TINYINT(2),
                crop_date_end TINYINT(2),
                gpx_id VARCHAR(50),
                INDEX idx_rsbsa_no (rsbsa_no),
                INDEX idx_parcel_id (parcel_id)
            )`;
        await this.targetPool.query(sql);
    }

    async createFarmParcelAttachments() {
        const sql = `
            CREATE TABLE IF NOT EXISTS farmparcelattachments (
                att_id INT(11) AUTO_INCREMENT PRIMARY KEY,
                parcel_id VARCHAR(50),
                rsbsa_no VARCHAR(50),
                file_name VARCHAR(200),
                active ENUM('1','0'),
                encoder_agency VARCHAR(50),
                encoder_id VARCHAR(50),
                encoder_fullname VARCHAR(200),
                date_created TIMESTAMP,
                INDEX idx_rsbsa_no (rsbsa_no),
                INDEX idx_parcel_id (parcel_id)
            )`;
        await this.targetPool.query(sql);
    }

    async createFarmParcel() {
        const sql = `
            CREATE TABLE IF NOT EXISTS farmparcel (
                parcel_id VARCHAR(50) PRIMARY KEY,
                parcel_no TINYINT(2),
                arb TINYINT(2),
                ancestral TINYINT(2),
                bgy1 TINYINT(3) UNSIGNED ZEROFILL,
                mun1 TINYINT(2) UNSIGNED ZEROFILL,
                prv1 TINYINT(2) UNSIGNED ZEROFILL,
                reg1 TINYINT(2) UNSIGNED ZEROFILL,
                geo_code VARCHAR(9),
                bgy INT(3) UNSIGNED ZEROFILL,
                mun INT(2) UNSIGNED ZEROFILL,
                prv INT(3) UNSIGNED ZEROFILL,
                reg INT(2) UNSIGNED ZEROFILL,
                desc_location VARCHAR(200),
                parcel_geo_pol POLYGON,
                parcel_geo_point POINT,
                lat FLOAT(10,0),
                \`long\` FLOAT(10,0),
                farm_area DECIMAL(10,4) UNSIGNED,
                temp_farm_area DECIMAL(10,4),
                unit_measure VARCHAR(20),
                own_doc TINYINT(2),
                own_doc_no VARCHAR(50),
                type TINYINT(1),
                owner_firstname VARCHAR(200),
                owner_lastname VARCHAR(200),
                owner_extname VARCHAR(200),
                owner_ans TINYINT(1),
                owner_rsbsa_no VARCHAR(50),
                farmers_rotation_fullname VARCHAR(200),
                farmers_rotation_rsbsa_no VARCHAR(200),
                remarks LONGTEXT,
                attachment VARCHAR(200),
                active ENUM('1','0'),
                date_created TIMESTAMP,
                slip_b_update TINYINT(4),
                from_slip_b_update TINYINT(4),
                INDEX idx_owner_rsbsa_no (owner_rsbsa_no)
            )`;
        await this.targetPool.query(sql);
    }

    async createFarmParcelOwnership() {
        const sql = `
            CREATE TABLE IF NOT EXISTS farmparcelownership (
                farmownID INT(11) AUTO_INCREMENT PRIMARY KEY,
                parcel_id VARCHAR(50),
                rsbsa_no VARCHAR(50),
                own_status VARCHAR(100),
                date_created TIMESTAMP,
                active ENUM('1','0'),
                encoder_agency VARCHAR(50),
                encoder_id VARCHAR(50),
                encoder_fullname VARCHAR(255),
                INDEX idx_rsbsa_no (rsbsa_no),
                INDEX idx_parcel_id (parcel_id)
            )`;
        await this.targetPool.query(sql);
    }

    async getValidLogRecords() {
        const [rows] = await this.sourcePool.query(
            `SELECT log_id, rsbsa_no, \`table\`
             FROM etl_logger_profiling
             WHERE rsbsa_no IS NOT NULL
               AND (\`table\` IS NOT NULL OR \`table\` = 'farmparcel')
             ORDER BY log_id ASC`
        );
        return rows;
    }

    async processBatch(batch) {
        let processedCount = 0;
        let skippedCount = 0;
        const batchErrors = [];
        const batchWarnings = [];

        // Group by table and rsbsa_no
        const groupedByTable = batch.reduce((acc, record) => {
            if (!record.table || !record.rsbsa_no) {
                batchWarnings.push({
                    log_id: record.log_id,
                    message: 'Skipped due to missing table or RSBSA number'
                });
                skippedCount++;
                return acc;
            }
            const key = `${record.table}:${record.rsbsa_no}`;
            if (!acc[record.table]) acc[record.table] = {};
            acc[record.table][key] = acc[record.table][key] || [];
            acc[record.table][key].push(record);
            return acc;
        }, {});

        for (const table of Object.keys(groupedByTable)) {
            const rsbsaNos = Object.keys(groupedByTable[table]).map(key => key.split(':')[1]);
            try {
                const sourceData = await this.getSourceData(table, rsbsaNos);
                if (!sourceData || sourceData.length === 0) {
                    batchWarnings.push(...rsbsaNos.map(rsbsa_no => ({
                        message: `No source data for RSBSA ${rsbsa_no} in ${table}`
                    })));
                    skippedCount += rsbsaNos.length;
                    continue;
                }

                await this.transferData(table, sourceData);
                processedCount += sourceData.length;

                if (table === 'farmparcelownership') {
                    const parcelIds = sourceData.map(item => item.parcel_id);
                    if (parcelIds.length > 0) {
                        const [parcelData] = await this.sourcePool.query(
                            `SELECT * FROM farmparcel WHERE parcel_id IN (?)`,
                            [parcelIds]
                        );
                        if (parcelData.length > 0) {
                            await this.transferData('farmparcel', parcelData);
                            processedCount += parcelData.length;
                        }
                    }
                }
            } catch (error) {
                const errorDetails = rsbsaNos.map(rsbsa_no => ({
                    rsbsa_no,
                    error: error.message,
                    stack: process.env.NODE_ENV === 'development' ? error.stack : undefined,
                    timestamp: new Date().toISOString()
                }));
                batchErrors.push(...errorDetails);
                skippedCount += rsbsaNos.length;
                logger.error(`Failed to process batch for table ${table}`, {
                    error: error.message,
                    stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
                });
            }
        }

        if (batchErrors.length > 0) {
            logger.error(`Batch completed with ${batchErrors.length} errors`, {
                sampleErrors: batchErrors.slice(0, 5),
                errorRate: `${((batchErrors.length / batch.length) * 100).toFixed(2)}%`
            });
            console.log(`Batch completed with ${batchErrors.length} errors: ${JSON.stringify(batchErrors.slice(0, 5))}`);
        }

        if (batchWarnings.length > 0) {
            logger.log(`Batch had ${batchWarnings.length} warnings`, {
                sampleWarnings: batchWarnings.slice(0, 5)
            });
            console.log(`Batch had ${batchWarnings.length} warnings: ${JSON.stringify(batchWarnings.slice(0, 5))}`);
        }

        return {
            processedCount,
            skippedCount,
            errors: batchErrors,
            warnings: batchWarnings
        };
    }

    async runEtlProcess() {
        try {
            this.lastRun = new Date();
            const batchSize = this.batchSize;
            let offset = 0;
            let totalProcessed = 0;
            let totalSkipped = 0;
            let lastLoggedProgress = -1;

            const totalRecords = await this.etlLogger.getTotalRecords();
            logger.log(`Starting RSBSA ETL. Total records: ${totalRecords}`);

            if (totalRecords === 0) {
                logger.log('No records to process for RSBSA ETL');
                return {
                    processed: 0,
                    skipped: 0,
                    startTime: this.lastRun,
                    endTime: new Date()
                };
            }

            while (offset < totalRecords) {
                const currentOffset = offset;
                logger.log(`Processing batch: ${currentOffset} to ${Math.min(currentOffset + batchSize - 1, totalRecords - 1)}`);

                const batch = await this.etlLogger.getRecordsBatch(currentOffset, batchSize);
                if (batch.length === 0) break;

                const result = await this.processBatch(batch);
                totalProcessed += result.processedCount;
                totalSkipped += result.skippedCount;
                offset += batchSize;

                const currentProgress = Math.min(Math.round((offset / totalRecords) * 100), 100);
                if (currentProgress > lastLoggedProgress) {
                    logger.log(`Progress: ${currentProgress}% (${Math.min(offset, totalRecords)}/${totalRecords})`);
                    lastLoggedProgress = currentProgress;
                }

                await new Promise(resolve => setTimeout(resolve, 100)); // Reduced delay
            }

            logger.log(`RSBSA ETL completed. Total Processed: ${totalProcessed}, Total Skipped: ${totalSkipped}`);
            return {
                processed: totalProcessed,
                skipped: totalSkipped,
                startTime: this.lastRun,
                endTime: new Date()
            };
        } catch (error) {
            logger.error('RSBSA ETL process failed', {
                error: error.message,
                stack: error.stack
            });
            throw error;
        }
    }

    getPHTTimestamp() {
        const now = new Date();
        const options = {
            timeZone: 'Asia/Manila',
            hour12: false,
            year: 'numeric',
            month: '2-digit',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
        };
        const parts = new Intl.DateTimeFormat('en-CA', options).formatToParts(now);
        const dateParts = {};
        parts.forEach(({ type, value }) => {
            dateParts[type] = value;
        });
        const { year, month, day, hour, minute, second } = dateParts;
        const ms = now.getMilliseconds().toString().padStart(3, '0');
        return `${year}-${month}-${day}T${hour}:${minute}:${second}.${ms}+08:00`;
    }

    async stopEtlProcess() {
        const stopTime = this.getPHTTimestamp();
        logger.log(`RSBSA ETL scheduler stopped at ${stopTime}`);

        if (this.lastRun) {
            const lastRunTime = new Date(this.lastRun);
            const formattedLastRun = lastRunTime.toLocaleString();
            logger.log(`Last ETL run was at: ${formattedLastRun}`);
            console.log(`Last ETL run was at: ${formattedLastRun}`);
        }

        return {
            message: 'ETL scheduler stopped successfully',
            stopTime: stopTime,
            lastRun: this.lastRun
        };
    }
}

module.exports = EtlService;