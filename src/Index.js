const WritableIndex = require('./Index/WritableIndex');
const ReadOnlyIndex = require('./Index/ReadOnlyIndex');

module.exports = WritableIndex;
module.exports.ReadOnly = ReadOnlyIndex;
module.exports.Entry = WritableIndex.Entry;