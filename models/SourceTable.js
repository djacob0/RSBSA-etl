class SourceTable {
    constructor(sourcePool, targetPool) {
        this.sourcePool = sourcePool;
        this.targetPool = targetPool;
    }
  
    async getDataByRsbsaNo(tableName, rsbsaNo) {
      const [rows] = await this.sourcePool.query(
        `SELECT * FROM ${tableName} WHERE rsbsa_no = ?`,
        [rsbsaNo]
      );
      return rows[0];
    }
  
    async transferData(sourceTable, targetTable, data) {
      const [existing] = await this.targetPool.query(
        `SELECT * FROM ${targetTable} WHERE rsbsa_no = ?`,
        [data.rsbsa_no]
      );
  
      if (existing.length > 0) {
        const setClause = Object.keys(data)
          .filter(key => key !== 'rsbsa_no')
          .map(key => `${key} = ?`)
          .join(', ');
        
        const values = Object.keys(data)
          .filter(key => key !== 'rsbsa_no')
          .map(key => data[key]);
        
        values.push(data.rsbsa_no);
        
        await this.targetPool.query(
          `UPDATE ${targetTable} SET ${setClause} WHERE rsbsa_no = ?`,
          values
        );
      } else {        const columns = Object.keys(data).join(', ');
        const placeholders = Object.keys(data).map(() => '?').join(', ');
        const values = Object.values(data);
        
        await this.targetPool.query(
          `INSERT INTO ${targetTable} (${columns}) VALUES (${placeholders})`,
          values
        );
      }
    }
  }
  
  module.exports = SourceTable;