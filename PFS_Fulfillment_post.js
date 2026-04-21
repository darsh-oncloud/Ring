/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 */
define(['N/search', 'N/record', 'N/log', 'N/runtime', 'N/task'], function(search, record, log, runtime, task) {

    var RECORD_TYPE = 'customrecord_inventory_snapshot_entry';
    var DATE_FIELD = 'custrecord_rc_ins_date';
    var CUT_OFF_DATE = '03/16/2026';
    var MAX_RECORDS = 50000;

    function getInputData() {
        var results = [];
        var totalLoaded = 0;

        var recSearch = search.create({
            type: RECORD_TYPE,
            filters: [
                [DATE_FIELD, 'before', CUT_OFF_DATE]
            ],
            columns: [
                search.createColumn({
                    name: DATE_FIELD,
                    sort: search.Sort.DESC
                }),
                search.createColumn({
                    name: 'internalid',
                    sort: search.Sort.DESC
                })
            ]
        });

        var pagedData = recSearch.runPaged({ pageSize: 1000 });

        for (var p = 0; p < pagedData.pageRanges.length; p++) {
            if (totalLoaded >= MAX_RECORDS) {
                break;
            }

            var page = pagedData.fetch({ index: p });

            for (var i = 0; i < page.data.length; i++) {
                if (totalLoaded >= MAX_RECORDS) {
                    break;
                }

                results.push(page.data[i].id);
                totalLoaded++;
            }
        }

        log.audit('Delete Batch Prepared', {
            cutOffDate: CUT_OFF_DATE,
            maxRecords: MAX_RECORDS,
            loadedForDeletion: results.length,
            deleteOrder: 'Most recent records before cutoff date first'
        });

        return results;
    }

    function map(context) {
        var recId = context.value;

        try {
            record.delete({
                type: RECORD_TYPE,
                id: recId
            });

            context.write({
                key: 'deleted',
                value: '1'
            });

        } catch (e) {
            log.error('Delete Error for Record ID ' + recId, e);

            context.write({
                key: 'error',
                value: recId
            });
        }
    }

    function summarize(summary) {
        var deletedCount = 0;
        var errorCount = 0;

        if (summary.inputSummary.error) {
            log.error('Input Error', summary.inputSummary.error);
        }

        summary.mapSummary.errors.iterator().each(function(key, error) {
            errorCount++;
            log.error('Map Error for Key: ' + key, error);
            return true;
        });

        summary.output.iterator().each(function(key, value) {
            if (key === 'deleted') {
                deletedCount += parseInt(value, 10) || 0;
            } else if (key === 'error') {
                errorCount++;
            }
            return true;
        });

        log.audit('Deletion Summary', {
            cutoffDate: CUT_OFF_DATE,
            deleted: deletedCount,
            errors: errorCount
        });

        if (deletedCount >= MAX_RECORDS) {
            try {
                var mrTask = task.create({
                    taskType: task.TaskType.MAP_REDUCE,
                    scriptId: runtime.getCurrentScript().id,
                    deploymentId: runtime.getCurrentScript().deploymentId
                });

                var taskId = mrTask.submit();

                log.audit('Map/Reduce Resubmitted', {
                    taskId: taskId,
                    cutOffDate: CUT_OFF_DATE,
                    maxRecords: MAX_RECORDS
                });

            } catch (e) {
                log.error('Resubmit Error', e);
            }
        } else {
            log.audit('Auto Resubmit Stopped', 'Less than batch size deleted, likely no more matching records remain.');
        }
    }

    return {
        getInputData: getInputData,
        map: map,
        summarize: summarize
    };
});