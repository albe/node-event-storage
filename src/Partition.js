const WritablePartition = require('./Partition/WritablePartition');
const ReadOnlyPartition = require('./Partition/ReadOnlyPartition');

module.exports = WritablePartition;
module.exports.ReadOnly = ReadOnlyPartition;