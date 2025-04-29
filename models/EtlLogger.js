class EtlLogger {
  constructor(pool) {
    this.pool = pool;
  }

  async getRecordsBatch(offset = 0, limit = 500) {
    const [rows] = await this.pool.query(
      `SELECT log_id, rsbsa_no, \`table\`
       FROM etl_logger_profiling
       WHERE rsbsa_no IS NOT NULL
         AND \`table\` IS NOT NULL
       ORDER BY log_id ASC
       LIMIT ? OFFSET ?`,
      [limit, offset]
    );
    return rows;
  }

  async getTotalRecords() {
    const [result] = await this.pool.query(
      `SELECT COUNT(*) as total
       FROM etl_logger_profiling
       WHERE rsbsa_no IS NOT NULL
         AND \`table\` IS NOT NULL`
    );
    return result[0].total;
  }
}

  module.exports = EtlLogger;