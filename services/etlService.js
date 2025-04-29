const EtlLogger = require('../models/EtlLogger');
const logger = require('../utils/logger');
const { sourcePool, targetPool } = require('../config/db');

class EtlService {
    constructor(sourcePool, targetPool) {
      this.sourcePool = sourcePool;
      this.targetPool = targetPool;
      this.etlLogger = new EtlLogger(sourcePool);
    }

    async getSourceData(table, rsbsaNo) {
      const [rows] = await this.sourcePool.query(
        `SELECT * FROM ${table} WHERE rsbsa_no = ?`,
        [rsbsaNo]
      );
      return rows[0];
    }

    async transferData(table, data) {
      await this.ensureTableExists(table);

      switch (table) {
        case 'farmers_kyc1':
          await this.handleFarmersKyc1(data);
          break;
        case 'farmers_kyc2':
          await this.handleFarmersKyc2(data);
          break;
        case 'farmers_kyc3':
          await this.handleFarmersKyc3(data);
          break;
        case 'farmers_kyc4':
          await this.handleFarmersKyc4(data);
          break;
        case 'farmers_attachments':
          await this.handleFarmersAttachments(data);
          break;
        case 'farmers_fca':
          await this.handleFarmersFca(data);
          break;
        case 'farmers_form_attachments':
          await this.handleFarmersFormAttachments(data);
          break;
        case 'farmers_livelihood':
          await this.handleFarmersLivelihood(data);
          break;
        case 'farmparcelactivity':
          await this.handleFarmParcelActivity(data);
          break;
        case 'farmparcelattachments':
          await this.handleFarmParcelAttachments(data);
          break;
        case 'farmparcelownership':
          await this.handleFarmParcelOwnership(data);
          break;
        default:
          await this.genericTransfer(table, data);
      }
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
        case 'farmparcelownership':
          await this.createFarmParcelOwnership();
          break;
      }
    }

    // Table creation methods
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
          v1_v2 TINYINT(1)
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
          emergency_contact VARCHAR(50)
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
          vtc_mafc_chair VARCHAR(255)
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
          philsys_last_user_fullname_liveness VARCHAR(100)
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
          encoder_fullname VARCHAR(255)
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
          encoder_fullname VARCHAR(255)
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
          encoder_fullname VARCHAR(255)
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
          active ENUM('1','0')
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
          gpx_id VARCHAR(50)
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
          date_created TIMESTAMP
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
          encoder_fullname VARCHAR(255)
        )`;
      await this.targetPool.query(sql);
    }

    // Data transfer handlers
    async handleFarmersKyc1(data) {
      const columns = [
        'farmerID', 'philsys_trn', 'philsys_pcn', 'sequence', 'rsbsa_no',
        'source_rsbsa_no', 'data_source', 'other_sys_gen_id', 'other_sys_id',
        'enrollment', 'file_picture', 'control_no', 'first_name', 'middle_name',
        'surname', 'ext_name', 'mother_maiden_name', 'spouse_rsbsa_no',
        'maiden_fname', 'maiden_mname', 'maiden_lname', 'maiden_extname',
        'sex', 'birthday', 'birth_place', 'birth_prv', 'birth_prv_mun',
        'house_no', 'street', 'brgy1', 'mun1', 'prv1', 'reg1', 'geo_code',
        'geocode', 'brgy', 'mun', 'prv', 'reg', 'ncr_brgy', 'ncr_mun',
        'ncr_prv', 'ncr_reg', 'ncr_house_no', 'ncr_street', 'c_date',
        'clone_by_id', 'clone_by_fullname', 'date_cloned', 'v1_v2'
      ];

      const processedData = this.uppercaseFields(data, [
        'data_source', 'first_name', 'middle_name', 'surname', 'ext_name',
        'mother_maiden_name', 'maiden_fname', 'maiden_mname', 'maiden_lname',
        'maiden_extname', 'birth_place', 'birth_prv', 'birth_prv_mun', 'street'
      ]);

      await this.handleTableTransfer('farmers_kyc1', columns, processedData);
    }

    async handleFarmersKyc2(data) {
      const columns = [
        'rsbsa_no', 'contact_num', 'contact_num_question', 'mob_number_fname',
        'mob_number_mname', 'mob_number_lname', 'mob_number_extname',
        'landline_num', 'education', 'pwd', 'religion', 'civil_status',
        'spouse', 'spouse_fname', 'spouse_mname', 'spouse_lname',
        'spouse_extname', 'spouse_rsbsa_no', 'beneficiary_4ps', 'ind_ans',
        'ind_id', 'gov_ans', 'gov_id', 'gov_id_num', 'hh_head', 'hh_head_name',
        'hh_relationship', 'hh_no_members', 'hh_no_male', 'hh_no_female',
        'fca_ans', 'fca_id', 'emergency_name', 'emergency_contact'
      ];

      const processedData = this.uppercaseFields(data, [
        'mob_number_fname', 'mob_number_mname', 'mob_number_lname',
        'mob_number_extname', 'spouse', 'hh_head_name', 'hh_relationship',
        'emergency_name'
      ]);

      await this.handleTableTransfer('farmers_kyc2', columns, processedData);
    }

    async handleFarmersKyc3(data) {
      const columns = [
        'rsbsa_no', 'no_farm_parcels', 'arb', 'gross_income_farming',
        'gross_income_nonfarming', 'vtc_date', 'vtc_bgy_chair',
        'vtc_agri_office', 'vtc_mafc_chair'
      ];

      const processedData = this.uppercaseFields(data, [
        'vtc_bgy_chair', 'vtc_agri_office', 'vtc_mafc_chair'
      ]);

      await this.handleTableTransfer('farmers_kyc3', columns, processedData);
    }

    async handleFarmersKyc4(data) {
      const columns = [
        'rsbsa_no', 'encoder_agency', 'encoder_id', 'encoder_fullname',
        'encoder_id_updated', 'encoder_fullname_updated', 'date_created',
        'date_updated', 'deceased', 'deceased_reason', 'ch_occupation',
        'ch_occupation_reason', 'duplicated', 'duplicated_reason',
        'duplicated_rsbsa_no', 'rffa2_cashout', 'validated',
        'unvalidated_reason', 'validator_by_id', 'validator_fullname',
        'date_validated', 'submitted', 'date_submitted', 'submitted_by_id',
        'submitted_by_fullname', 'rfo_validated', 'rfo_date_validated',
        'rfo_validated_id', 'rfo_validated_fullname', 'online_applicant',
        'checked_date', 'checked', 'checked_by_id', 'checked_fullname',
        'complete_cloned_by_fullname', 'complete_cloned_by_id',
        'date_cloned_completed', 'rsbsa_liveness_verified',
        'rsbsa_last_liveness_date', 'rsbsa_last_user_id_liveness',
        'rsbsa_last_user_fullname_liveness', 'philsys_liveness_verified',
        'philsys_last_liveness_date', 'philsys_last_user_id_liveness',
        'philsys_last_user_fullname_liveness'
      ];

      const processedData = this.uppercaseFields(data, [
        'encoder_fullname', 'encoder_fullname_updated', 'deceased_reason'
      ]);

      await this.handleTableTransfer('farmers_kyc4', columns, processedData);
    }

    async handleFarmersAttachments(data) {
      const columns = [
        'rsbsa_no', 'filename', 'validity_file', 'date_created',
        'active', 'encoder_agency', 'encoder_id', 'encoder_fullname'
      ];

      const processedData = this.uppercaseFields(data, ['encoder_fullname']);
      await this.handleTableTransfer('farmers_attachments', columns, processedData);
    }

    async handleFarmersFca(data) {
      const columns = [
        'rsbsa_no', 'fca_id', 'fca_name', 'date_created',
        'active', 'encoder_agency', 'encoder_id', 'encoder_fullname'
      ];

      const processedData = this.uppercaseFields(data, ['fca_name', 'encoder_fullname']);
      await this.handleTableTransfer('farmers_fca', columns, processedData);
    }

    async handleFarmersFormAttachments(data) {
      const columns = [
        'rsbsa_no', 'filename', 'date_created', 'active',
        'encoder_agency', 'encoder_id', 'encoder_fullname'
      ];

      const processedData = this.uppercaseFields(data, ['encoder_fullname']);
      await this.handleTableTransfer('farmers_form_attachments', columns, processedData);
    }

    async handleFarmersLivelihood(data) {
      const columns = [
        'rsbsa_no', 'livelihood', 'activity_work', 'specify', 'active'
      ];

      const processedData = this.uppercaseFields(data, [
        'livelihood', 'activity_work', 'specify'
      ]);

      await this.handleTableTransfer('farmers_livelihood', columns, processedData);
    }

    async handleFarmParcelActivity(data) {
      const columns = [
        'parcel_id', 'rsbsa_no', 'crop_id', 'size', 'temp_size', 'orig',
        'no_heads', 'farm_type', 'organic', 'active', 'encoder_agency',
        'encoder_id', 'encoder_fullname', 'date_created', 'slip_b_update',
        'from_slip_b_update', 'intercrop', 'crop_date_start', 'crop_date_end',
        'gpx_id'
      ];

      const processedData = this.uppercaseFields(data, ['encoder_agency', 'encoder_fullname']);
      await this.handleTableTransfer('farmparcelactivity', columns, processedData);
    }

    async handleFarmParcelAttachments(data) {
      const columns = [
        'parcel_id', 'rsbsa_no', 'file_name', 'active', 'encoder_agency',
        'encoder_id', 'encoder_fullname', 'date_created'
      ];

      const processedData = this.uppercaseFields(data, ['encoder_agency', 'encoder_fullname']);
      await this.handleTableTransfer('farmparcelattachments', columns, processedData);
    }

    async handleFarmParcelOwnership(data) {
      const columns = [
        'parcel_id', 'rsbsa_no', 'own_status', 'date_created', 'active',
        'encoder_agency', 'encoder_id', 'encoder_fullname'
      ];

      const processedData = this.uppercaseFields(data, [
        'own_status', 'encoder_agency', 'encoder_fullname'
      ]);

      await this.handleTableTransfer('farmparcelownership', columns, processedData);
    }

    uppercaseFields(data, fields) {
      const processed = {...data};
      fields.forEach(field => {
        if (processed[field] && typeof processed[field] === 'string') {
          processed[field] = processed[field].toUpperCase();
        }
      });
      return processed;
    }

    async handleTableTransfer(table, columns, data) {
      const [existing] = await this.targetPool.query(
        `SELECT 1 FROM ${table} WHERE rsbsa_no = ? LIMIT 1`,
        [data.rsbsa_no]
      );

      const validColumns = columns.filter(col => data[col] !== undefined);

      if (existing.length > 0) {
        const setClause = validColumns
          .filter(col => col !== 'rsbsa_no')
          .map(col => `${col} = ?`)
          .join(', ');

        const values = validColumns
          .filter(col => col !== 'rsbsa_no')
          .map(col => data[col]);

        values.push(data.rsbsa_no);

        await this.targetPool.query(
          `UPDATE ${table} SET ${setClause} WHERE rsbsa_no = ?`,
          values
        );
      } else {
        const placeholders = validColumns.map(() => '?').join(', ');
        const values = validColumns.map(col => data[col]);

        await this.targetPool.query(
          `INSERT INTO ${table} (${validColumns.join(', ')}) VALUES (${placeholders})`,
          values
        );
      }
    }

    // async genericTransfer(table, data) {
    //   const [existing] = await this.targetPool.query(
    //     `SELECT 1 FROM ${table} WHERE rsbsa_no = ? LIMIT 1`,
    //     [data.rsbsa_no]
    //   );

    //   if (existing.length > 0) {
    //     const setClause = Object.keys(data)
    //       .filter(key => key !== 'rsbsa_no')
    //       .map(key => `${key} = ?`)
    //       .join(', ');

    //     const values = Object.keys(data)
    //       .filter(key => key !== 'rsbsa_no')
    //       .map(key => data[key]);

    //     values.push(data.rsbsa_no);

    //     await this.targetPool.query(
    //       `UPDATE ${table} SET ${setClause} WHERE rsbsa_no = ?`,
    //       values
    //     );
    //   } else {
    //     const columns = Object.keys(data).join(', ');
    //     const placeholders = Object.keys(data).map(() => '?').join(', ');
    //     const values = Object.values(data);

    //     await this.targetPool.query(
    //       `INSERT INTO ${table} (${columns}) VALUES (${placeholders})`,
    //       values
    //     );
    //   }
    // }

    async getValidLogRecords() {
      const [rows] = await this.sourcePool.query(
      `SELECT log_id, rsbsa_no, \`table\`
        FROM etl_logger_profiling
        WHERE rsbsa_no IS NOT NULL
          AND \`table\` IS NOT NULL
        ORDER BY log_id ASC`
      );
      return rows;
    }

    async processBatch(batch) {
      let processedCount = 0;
      let skippedCount = 0;
      const batchErrors = [];
      const batchWarnings = [];

      for (const record of batch) {
        try {
          if (!record.table || !record.rsbsa_no) {
            batchWarnings.push({
              log_id: record.log_id,
              message: 'Skipped due to missing table or RSBSA number'
            });
            skippedCount++;
            continue;
          }

          const sourceData = await this.getSourceData(
            record.table,
            record.rsbsa_no
          );

          if (!sourceData) {
            batchWarnings.push({
              log_id: record.log_id,
              message: `No source data for RSBSA ${record.rsbsa_no} in ${record.table}`
            });
            skippedCount++;
            continue;
          }

          await this.transferData(record.table, sourceData);
          processedCount++;

        } catch (error) {
          batchErrors.push({
            log_id: record.log_id,
            error: error.message,
            stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
          });
          skippedCount++;
        }
      }

      if (batchErrors.length > 0) {
        logger.error(`Batch completed with ${batchErrors.length} errors`, {
          sampleErrors: batchErrors.slice(0, 5),
          errorRate: `${((batchErrors.length / batch.length) * 100).toFixed(2)}%`
        });
      }

      if (batchWarnings.length > 0) {
        logger.log(`Batch had ${batchWarnings.length} warnings`, {
          sampleWarnings: batchWarnings.slice(0, 5)
        });
      }

      logger.debug(`Batch stats: ${processedCount} processed, ${skippedCount} skipped`);

      return {
        processedCount,
        skippedCount,
        errors: batchErrors,
        warnings: batchWarnings
      };
    }

    async runEtlProcess() {
      try {
        const batchSize = 500;
        let offset = 0;
        let totalProcessed = 0;
        let totalSkipped = 0;

        const totalRecords = await this.etlLogger.getTotalRecords();

        while (offset < totalRecords) {
          logger.log(`Processing batch: ${offset} to ${offset + batchSize - 1}`);

          const batch = await this.etlLogger.getRecordsBatch(offset, batchSize);
          if (batch.length === 0) break;

          const { processedCount, skippedCount } = await this.processBatch(batch);

          totalProcessed += processedCount;
          totalSkipped += skippedCount;
          offset += batchSize;

          await new Promise(resolve => setTimeout(resolve, 1000));
        }

        logger.log(`ETL process completed. Total Processed: ${totalProcessed}, Total Skipped: ${totalSkipped}`);
        return { processed: totalProcessed, skipped: totalSkipped };
      } catch (error) {
        logger.error(`ETL process failed: ${error.message}`);
        throw error;
      }
    }
  }

module.exports = EtlService;