/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 */
define(['N/file', 'N/search', 'N/log'], function (file, search, log) {

    var FOLDER_ID = 2249;

    function getInputData() {
        var inputFileId = getLatestCsvFileFromFolder();

        if (!inputFileId) {
            throw 'No CSV file found in folder ' + FOLDER_ID;
        }

        var csvFile = file.load({ id: inputFileId });
        var contents = csvFile.getContents();
        var lines = contents.split(/\r?\n/);

        var data = [];

        for (var i = 1; i < lines.length; i++) {
            if (!lines[i] || lines[i].trim() === '') continue;

            var cols = parseCSVLine(lines[i]);

            data.push({
                lineNo: i,
                line: lines[i],
                pfsItemNumber: cols[0] || '',
                fileBarcode: cols[3] || ''
            });
        }

        log.audit('Input File ID', inputFileId);
        log.audit('Total Rows', data.length);

        return data;
    }

    function map(context) {
        var row = JSON.parse(context.value);

        var pfsItemNumber = trimValue(row.pfsItemNumber);
        var fileBarcode = trimValue(row.fileBarcode);

        var nsBarcode = '';
        var status = '';
        var errorMsg = '';

        try {
            if (!pfsItemNumber) {
                status = 'ERROR';
                errorMsg = 'PFS Item Number is empty';
            } else {
                var itemInfo = findItemBarcode(pfsItemNumber);

                if (!itemInfo.found) {
                    status = 'ITEM NOT FOUND';
                    errorMsg = 'No item found for PFS Item Number: ' + pfsItemNumber;
                } else {
                    nsBarcode = trimValue(itemInfo.barcode);
                    status = nsBarcode === fileBarcode ? 'MATCHED' : 'NOT MATCHED';
                }
            }
        } catch (e) {
            status = 'ERROR';
            errorMsg = e.message || e.toString();
        }

        context.write({
            key: row.lineNo,
            value: {
                line: row.line,
                nsBarcode: nsBarcode,
                status: status,
                error: errorMsg
            }
        });
    }

    function summarize(summary) {
        var inputFileId = getLatestCsvFileFromFolder();
        var inputFile = file.load({ id: inputFileId });

        var lines = inputFile.getContents().split(/\r?\n/);
        var header = parseCSVLine(lines[0]);

        header.push('NetSuite Barcode');
        header.push('Match Status');
        header.push('Error');

        var resultByLine = {};

        summary.output.iterator().each(function (key, value) {
            resultByLine[key] = JSON.parse(value);
            return true;
        });

        var outputLines = [];
        outputLines.push(toCSVLine(header));

        for (var i = 1; i < lines.length; i++) {
            if (!lines[i] || lines[i].trim() === '') continue;

            var cols = parseCSVLine(lines[i]);
            var result = resultByLine[i];

            if (result) {
                cols.push(result.nsBarcode || '');
                cols.push(result.status || '');
                cols.push(result.error || '');
            } else {
                cols.push('');
                cols.push('ERROR');
                cols.push('No result returned from Map stage');
            }

            outputLines.push(toCSVLine(cols));
        }

        var outputFile = file.create({
            name: 'barcode_validation_result_' + getDateTimeStamp() + '.csv',
            fileType: file.Type.CSV,
            contents: outputLines.join('\n'),
            folder: FOLDER_ID
        });

        var outputFileId = outputFile.save();

        log.audit('Output File Created', outputFileId);

        summary.mapSummary.errors.iterator().each(function (key, error) {
            log.error('Map Error Line ' + key, error);
            return true;
        });
    }

    function getLatestCsvFileFromFolder() {
        var latestFileId = '';

        var fileSearch = search.create({
            type: 'file',
            filters: [
                ['folder', 'anyof', FOLDER_ID],
                'AND',
                ['filetype', 'anyof', 'CSV']
            ],
            columns: [
                search.createColumn({
                    name: 'internalid',
                    sort: search.Sort.DESC
                })
            ]
        });

        var results = fileSearch.run().getRange({
            start: 0,
            end: 1
        });

        if (results && results.length > 0) {
            latestFileId = results[0].getValue({ name: 'internalid' });
        }

        return latestFileId;
    }

    function findItemBarcode(pfsItemNumber) {
        var obj = {
            found: false,
            barcode: ''
        };

        var itemSearch = search.create({
            type: search.Type.ITEM,
            filters: [
                ['custitem_ring_shopify_item_id', 'is', pfsItemNumber],
                'AND',
                ['isinactive', 'is', 'F']
            ],
            columns: [
                search.createColumn({ name: 'custitem_ag_barcode' })
            ]
        });

        var results = itemSearch.run().getRange({
            start: 0,
            end: 1
        });

        if (results && results.length > 0) {
            obj.found = true;
            obj.barcode = results[0].getValue({
                name: 'custitem_ag_barcode'
            }) || '';
        }

        return obj;
    }

    function parseCSVLine(line) {
        var result = [];
        var current = '';
        var insideQuotes = false;

        line = line || '';

        for (var i = 0; i < line.length; i++) {
            var ch = line[i];

            if (ch === '"') {
                if (insideQuotes && line[i + 1] === '"') {
                    current += '"';
                    i++;
                } else {
                    insideQuotes = !insideQuotes;
                }
            } else if (ch === ',' && !insideQuotes) {
                result.push(current);
                current = '';
            } else {
                current += ch;
            }
        }

        result.push(current);
        return result;
    }

    function toCSVLine(arr) {
        var output = [];

        for (var i = 0; i < arr.length; i++) {
            var value = arr[i];

            if (value == null) value = '';

            value = String(value);

            if (value.indexOf('"') !== -1) {
                value = value.replace(/"/g, '""');
            }

            if (
                value.indexOf(',') !== -1 ||
                value.indexOf('"') !== -1 ||
                value.indexOf('\n') !== -1 ||
                value.indexOf('\r') !== -1
            ) {
                value = '"' + value + '"';
            }

            output.push(value);
        }

        return output.join(',');
    }

    function trimValue(value) {
        if (value == null) return '';
        return String(value).trim();
    }

    function getDateTimeStamp() {
        var d = new Date();

        return d.getFullYear() +
            pad(d.getMonth() + 1) +
            pad(d.getDate()) + '_' +
            pad(d.getHours()) +
            pad(d.getMinutes()) +
            pad(d.getSeconds());
    }

    function pad(n) {
        return n < 10 ? '0' + n : String(n);
    }

    return {
        getInputData: getInputData,
        map: map,
        summarize: summarize
    };
});