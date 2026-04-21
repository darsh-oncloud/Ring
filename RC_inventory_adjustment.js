/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 */
define(['N/search', 'N/record', 'N/log'], function (search, record, log) {

  const ACCOUNT_ID = 466;
  const SUBSIDIARY_ID = 1;
  const EPS = 1e-6; // tolerance to treat near-equal quantities as equal

  function getInputData() {
    log.audit('getInputData', 'Loading snapshot records with status != 1 and quantity on hand provided');

    return search.create({
      type: 'customrecord_inventory_snapshot_entry',
      filters: [
        ['custrecord_rc_ins_status', 'anyof', '2'],
        'AND',
        ['custrecord_qunatity_on_hand', 'isnotempty', ''],
        'AND', 
        ['custrecord_rc_ins_item.custitem_rc_ghost_items', 'is', 'F'],
        'AND',
        ['custrecord_rc_ins_location', 'noneof', '@NONE@'],
        'AND',
        ['isinactive', 'is', 'F'] // skip already-inactive snapshots
      ],
      columns: [
        'internalid',
        'custrecord_rc_ins_item',
        'custrecord_qunatity_on_hand',
        'custrecord_rc_ins_date',
        'custrecord_rc_ins_location'
      ]
    });
  }

  function map(context) {
    try {
      const result = JSON.parse(context.value);
      const recId = result.id;
      const values = result.values;

      const itemId = (values.custrecord_rc_ins_item && values.custrecord_rc_ins_item.value) || null;
      const targetQty = parseFloat(values.custrecord_qunatity_on_hand) || 0;
      const trandateRaw = values.custrecord_rc_ins_date;
      const locationId = (values.custrecord_rc_ins_location && values.custrecord_rc_ins_location.value) || null;

      if (!itemId || !locationId || isNaN(targetQty)) {
        log.error('Missing item or quantity', { recId, itemId, targetQty, locationId });
        return;
      }

      // --- Get current location quantity on hand ---
      let currentQty = 0;
      try {
        const itemSearch = search.create({
          type: 'item',
          filters: [
            ['internalid', 'anyof', itemId],
            'AND',
            ['inventorylocation', 'anyof', locationId]
          ],
          columns: [
            search.createColumn({ name: 'locationquantityonhand' })
          ]
        });

        const resultSet = itemSearch.run().getRange({ start: 0, end: 1 });
        if (resultSet && resultSet.length) {
          currentQty = parseFloat(resultSet[0].getValue({ name: 'locationquantityonhand' })) || 0;
        }
      } catch (e) {
        log.error('Failed to get inventory quantity', e.message);
        return;
      }

      const diffRaw = targetQty - currentQty;
      const diff = Math.abs(diffRaw) < EPS ? 0 : diffRaw;

      if (diff === 0) {
        log.audit('No adjustment needed', { recId, itemId, targetQty, currentQty });
        // Now ALSO inactivate matched records
        markAsProcessed(recId, true);
        return;
      }

      // --- Create inventory adjustment for the delta ---
      try {
        const adj = record.create({
          type: record.Type.INVENTORY_ADJUSTMENT,
          isDynamic: true
        });

        adj.setValue('subsidiary', SUBSIDIARY_ID);
        adj.setValue('account', ACCOUNT_ID);

        const jsDate = new Date(trandateRaw);
        if (!isNaN(jsDate.getTime())) {
          adj.setValue('trandate', jsDate);
        }

        adj.setValue('memo', `Adjustment from Snapshot ${recId}`);

        adj.selectNewLine({ sublistId: 'inventory' });
        adj.setCurrentSublistValue({ sublistId: 'inventory', fieldId: 'item', value: itemId });
        adj.setCurrentSublistValue({ sublistId: 'inventory', fieldId: 'location', value: locationId });
        adj.setCurrentSublistValue({ sublistId: 'inventory', fieldId: 'adjustqtyby', value: diff });
        adj.commitLine({ sublistId: 'inventory' });

        const adjId = adj.save();
        log.audit('Inventory Adjustment Created', { recId, adjId, diff });

        // Processed + inactivate (IA created successfully)
        markAsProcessed(recId, true);

      } catch (e) {
        log.error('Failed to save Inventory Adjustment', e.message);
        // do NOT mark processed/inactive so it can be retried
      }
    } catch (e) {
      log.error('Unexpected Error in map()', e.message);
    }
  }

  /**
   * Mark snapshot as processed; optionally set it inactive.
   * @param {number|string} recId
   * @param {boolean} inactive - true to set isinactive = T
   */
  function markAsProcessed(recId, inactive) {
    try {
      const values = { custrecord_rc_ins_status: 1 };
      if (inactive === true) {
        values.isinactive = true; // standard field on custom records
      }

      record.submitFields({
        type: 'customrecord_inventory_snapshot_entry',
        id: recId,
        values: values
      });

      log.audit('Snapshot marked processed', { recId, inactive: !!inactive });
    } catch (e) {
      log.error('Failed to update snapshot record ' + recId, e.message);
    }
  }

  return {
    getInputData,
    map
  };
});