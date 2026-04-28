/**
 * ===========================
 * FILE 1: MR SCRIPT (FULL UPDATED)
 * ===========================
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 * @NModuleScope SameAccount
 */

define(['N/record','N/search','N/log','N/runtime','./RCShopifyIntegration'],
function(record, search, log, runtime, shopify) {

  var FULL_SEARCH_ID  = 'customsearch3112_2';
  var PRICE_SEARCH_ID = 'customsearch3239';

  function csvToArray(s){
    return !s ? [] : String(s).split(',').map(function(x){return x.trim();}).filter(Boolean);
  }

  function toNumberOrNull(v){
    if (v === null || v === undefined) return null;
    var s = String(v).trim();
    if (s === '') return null;
    var n = parseFloat(s);
    return isFinite(n) ? n : null;
  }

  function getInputData() {
    var searchId = runtime.getCurrentScript().getParameter({ name: 'custscript_shopify_search_id' });
    if (!searchId) throw new Error('Missing parameter: custscript_shopify_search_id');

    var rows = [];
    var searchObj = search.load({ id: searchId });

    var pagedData = searchObj.runPaged({ pageSize: 1000 });
    log.audit('getInputData', 'Search=' + searchId + ' Total Results=' + pagedData.count);

    pagedData.pageRanges.forEach(function(pageRange){
      var page = pagedData.fetch({ index: pageRange.index });
      page.data.forEach(function(result){
        var row = {};
        result.columns.forEach(function(col){
          var label = col.label || col.name;
          var val = result.getValue(col);
          var txt = result.getText(col);

          // keep the “val else txt” behavior you had
          row[label] = (val != null && val !== '') ? val : txt;

          // keep option mapping exactly
          if (col.name === 'custitem_shopify_option_1')       row.custitem_shopify_option_1 = val || txt || '';
          if (col.name === 'custitem_shopify_option_2')       row.custitem_shopify_option_2 = val || txt || '';
          if (col.name === 'custitem_shopify_option_3')       row.custitem_shopify_option_3 = val || txt || '';

          if (col.name === 'custitem_shopify_option_1_value') row.custitem_shopify_option_1_value = val || txt || '';
          if (col.name === 'custitem_shopify_option_2_value') row.custitem_shopify_option_2_value = val || txt || '';
          if (col.name === 'custitem_shopify_option_3_value') row.custitem_shopify_option_3_value = val || txt || '';
        });
        rows.push(row);
      });
    });

    return rows;
  }

  function map(context) {
    var searchId = String(runtime.getCurrentScript().getParameter({ name: 'custscript_shopify_search_id' }) || '');
    var isPriceRun = (searchId === PRICE_SEARCH_ID);

    var r = JSON.parse(context.value);

    var itemId      = r["internalid"];
    var sku         = r["itemid"];
    var type        = r["Type"];
    var displayName = r["Display Name"] || r["displayname"] || sku;

    var productId  = r["product_id"];
    var variantId  = r["variant_id"];

    var parentInternalId = r["Parent (Internal ID)"] || null;
    var parentItemId = parentInternalId ? String(parentInternalId) : String(itemId);

    // For child rows, search should provide parent product id in this column:
    var parentProductId  = r["Parent Product ID"] || null;

    var mf = {
      _giftwrap: r["_giftwrap"],
      final_sale: r["final_sale"],
      custom_sale_price: r["custom_sale_price"],
      vip_price_sale_: r["vip_price_sale_"],
      vvip_price_sale_: r["vvip_price_sale_"]
    };

    var shopifyLocIds = r["SHOPIFY LOCATION ID"] || '';
    var costPerItem = r["Cost Per Item"];
    // -------------------------
    // PRICE RUN
    // -------------------------
    if (isPriceRun) {
      var price          = toNumberOrNull(r["Price (Variant)"]);
      var compareAtPrice = toNumberOrNull(r["compare_at_price"]);

      // For pricing, always group by parent product id
      // (child row => Parent Product ID, parent row => product_id)
      var pid = parentProductId || productId || null;

      if (!pid) {
        log.debug('PRICE:skip-no-productId', { itemId: itemId, sku: sku, variantId: variantId });
        return;
      }
      if (price === null) {
        log.debug('PRICE:skip-no-price', { itemId: itemId, sku: sku, productId: pid, variantId: variantId });
        return;
      }

      context.write({
        key: 'PRICE|' + String(pid),
        value: JSON.stringify({
          itemId: itemId,
          type: type,
          sku: sku,
          productId: String(pid),
          variantId: variantId ? String(variantId) : null,
          price: price,
          costPerItem: costPerItem,
          compareAtPrice: (compareAtPrice === null ? null : compareAtPrice)
        })
      });
      return;
    }

    // -------------------------
    // FULL RUN (split logic):
    //  - DO NOT group parent+child
    //  - Each item is processed alone in reduce
    // -------------------------
    context.write({
      key: 'FULLITEM|' + String(itemId),
      value: JSON.stringify({
        itemId: itemId,
        parentItemId: parentItemId,
        isParent: (!parentInternalId),

        sku: sku,
        type: type,
        displayName: displayName,

        // If your search puts parent product id directly for parent row, keep productId
        productId: productId || null,
        variantId: variantId || null,

        // For child rows, this must be the PARENT product id column
        parentProductId: parentProductId || null,
        barcode: (r["BARCODE"] || '').trim(),

        // --- ADDED MAPPINGS FOR SHIPPING SYNC ---
        countryOfOrigin: r["countryofmanufacture"] || r["Manufacturer Country"] || '',
        hsCode: r["manufacturertariff"] || r["Manufacturer Tariff"] || '',

        o1: (r.custitem_shopify_option_1 || '').trim(),
        o2: (r.custitem_shopify_option_2 || '').trim(),
        o3: (r.custitem_shopify_option_3 || '').trim(),
        o1v: (r.custitem_shopify_option_1_value || ''),
        o2v: (r.custitem_shopify_option_2_value || ''),
        o3v: (r.custitem_shopify_option_3_value || ''),

        metafieldInput: mf,

        // ONLY use per-row locations
        shopifyLocIds: shopifyLocIds,
        costPerItem: costPerItem
      })
    });
  }

  function reduce(context) {
    var key = String(context.key || '');

    // ===================================================
    // PRICE reduce (UPDATED to BULK, nothing else changed)
    // ===================================================
    if (key.indexOf('PRICE|') === 0) {
      var productIdStr = key.split('|')[1] || '';
      var rows = context.values.map(function(s){ return JSON.parse(s); });

      function getDefaultVariantId(productId) {
        try {
          var prod = shopify.getProduct(String(productId));
          var v = prod && prod.variants && prod.variants[0] ? prod.variants[0] : null;
          return v && v.id ? String(v.id) : null;
        } catch (e) {
          log.error('PRICE:getDefaultVariantId-failed', {
            productId: productId,
            msg: (e && e.message) ? e.message : String(e),
            name: (e && e.name) ? e.name : ''
          });
          return null;
        }
      }

      var ok = 0;
      var failed = 0;
      var defaultVariantId = null;

      // Build bulk payload for this product
      var updates = [];
      var toUncheck = []; // [{ itemId, type }]
      var updateItems = []; // same index as updates
      for (var i = 0; i < rows.length; i++) {
        var r = rows[i];

        if (r.price === null || r.price === undefined || r.price === '') {
          log.debug('PRICE:skip-empty-price', { productId: productIdStr, itemId: r.itemId });
          continue;
        }

        var vid = r.variantId;
        if (!vid) {
          if (!defaultVariantId) defaultVariantId = getDefaultVariantId(productIdStr);
          vid = defaultVariantId;
        }

        if (!vid) {
          failed++;
          log.error('PRICE:skip-no-variant', { productId: productIdStr, itemId: r.itemId, sku: r.sku });
          continue;
        }

        updates.push({
          productId: String(productIdStr),
          variantId: String(vid),
          price: r.price,
          compareAtPrice: (r.compareAtPrice === null ? null : r.compareAtPrice)
        });

        updateItems.push({
          itemId: r.itemId,
          type: r.type,
          sku: r.sku,
          variantId: String(vid)
        });

        toUncheck.push({ itemId: r.itemId, type: r.type });
      }

      if (!updates.length) {
        log.audit('PRICE:done', { productId: productIdStr, totalRows: rows.length, ok: ok, failed: failed });
        return;
      }


      log.audit('PRICE:updates', {
      productId: productIdStr,
      updates: updates
      });
      
      // One bulk call (fast) - requires library function bulkUpdatePricesFast(updates)
      var bulkRes = null;
      try {
        bulkRes = shopify.bulkUpdatePricesFast(updates);
      } catch (eBulk) {
        log.error('PRICE:bulkUpdatePricesFast-throw', {
          productId: productIdStr,
          msg: (eBulk && eBulk.message) ? eBulk.message : String(eBulk),
          name: (eBulk && eBulk.name) ? eBulk.name : ''
        });
        log.audit('PRICE:done', { productId: productIdStr, totalRows: rows.length, ok: ok, failed: (failed + updates.length) });
        return;
      }

      if (!bulkRes || bulkRes.ok !== true) {
        failed += updates.length;
        log.error('PRICE:bulk-failed', { productId: productIdStr, res: bulkRes });

        try {
          var failedProducts = (bulkRes && bulkRes.failedProducts) || [];
          var userErrors = (failedProducts[0] && failedProducts[0].userErrors) || [];

          for (var e = 0; e < userErrors.length; e++) {
            var err = userErrors[e];
            var fieldArr = err.field || [];
            var msg = err.message || '';
            var idx = null;

            if (fieldArr.length >= 2 && fieldArr[0] === 'variants') {
              idx = parseInt(fieldArr[1], 10);
            }

            if (
              msg === 'Product variant does not exist' &&
              idx !== null &&
              !isNaN(idx) &&
              updateItems[idx]
            ) {
              var badRow = updateItems[idx];

              log.audit('PRICE:item-checkbox-unchecked', {
                productId: productIdStr,
                index: idx,
                itemId: badRow.itemId,
                sku: badRow.sku,
                variantId: badRow.variantId,
                reason: msg
              });

              record.submitFields({
                type: shopify.getType(badRow.type),
                id: badRow.itemId,
                values: {
                  custitem_shopify_price_update: false
                }
              });
            }
          }
        } catch (mapErr) {
          log.error('PRICE:error-mapping-failed', mapErr);
        }

        log.audit('PRICE:done', { productId: productIdStr, totalRows: rows.length, ok: ok, failed: failed });
        return;
      }

      // ✅ write uncheck tasks to summarize (do NOT submitFields here)
      for (var u = 0; u < toUncheck.length; u++) {
        context.write({
          key: 'UNCK',
          value: JSON.stringify({
            itemId: toUncheck[u].itemId,
            type: toUncheck[u].type
          })
        });
        ok++;
      }

      log.audit('PRICE:done', {
        productId: productIdStr,
        totalRows: rows.length,
        bulkAttempted: updates.length,
        bulkUpdated: bulkRes.updated,
        ok: ok,
        failed: failed
      });
      return;
    }

    // ===================================================
    // FULLITEM reduce (split logic)
    // ===================================================
    if (key.indexOf('FULLITEM|') !== 0) return;

    // Only one row per key now
    var row = JSON.parse(context.values[0]);

    function buildMoneyValue(v, currencyCode) {
  if (v === null || v === undefined || String(v).trim() === '') return '';

  var n = parseFloat(String(v).replace(/,/g, '').trim());
  if (!isFinite(n)) return '';

  return JSON.stringify({
    amount: n.toFixed(2),
    currency_code: String(currencyCode || 'USD')
  });
}

function buildMetafields(mfi) {
  mfi = mfi || {};
  var STORE_CURRENCY = 'USD'; // change if your Shopify store currency is different

  return [
    { namespace: 'custom', key: '_giftwrap', value: (mfi._giftwrap === true || mfi._giftwrap === 'T') ? 'true' : 'false', type: 'boolean' },
    { namespace: 'custom', key: 'final_sale', value: (mfi.final_sale === true || mfi.final_sale === 'T') ? 'true' : 'false', type: 'boolean' },
    { namespace: 'custom', key: 'custom_sale_price_currency', value: buildMoneyValue(mfi.custom_sale_price, STORE_CURRENCY), type: 'money' },
    { namespace: 'custom', key: 'vip_price_sale_currency', value: buildMoneyValue(mfi.vip_price_sale_, STORE_CURRENCY), type: 'money' },
    { namespace: 'custom', key: 'vvip_price_sale_currency', value: buildMoneyValue(mfi.vvip_price_sale_, STORE_CURRENCY), type: 'money' }
  ];
}
    function parseLocs(locStr) { return csvToArray(locStr); }

    function getShopifyProductSafe(pid) {
      if (!pid) return null;
      try { return shopify.getProduct(String(pid)); } catch (e) { return null; }
    }
    function productExists(prod) {
      return !!(prod && (prod.id || prod.productId));
    }
    function variantExistsInProduct(prod, vid) {
      if (!prod || !vid) return false;
      var vars = prod.variants || [];
      for (var i = 0; i < vars.length; i++) {
        if (String(vars[i].id) === String(vid)) return true;
      }
      return false;
    }

    try {
      // -------------------------
      // PARENT
      // -------------------------
      if (row.isParent) {
        var parentMetafields = buildMetafields(row.metafieldInput);
        var parentLocs = parseLocs(row.shopifyLocIds);

        var pid = row.productId;

        // CREATE parent (NO PRICE, NO COMPARE)
        if (!pid) {
          var created = shopify.createSimpleProduct(
            row.displayName,
            row.sku,
            undefined, // price
            undefined, // compare_at
            parentMetafields
          );

          if (!created || !created.productId) {
            log.error('FULL:parent-create-failed', { itemId: row.itemId, sku: row.sku });
            return;
          }

          pid = String(created.productId);

          record.submitFields({
            type: shopify.getType(row.type),
            id: row.itemId,
            values: {
              custitem_rc_shopify_product_id: pid,
              custitem_ring_shopify_item_id: '',
              custitem_rc_send_to_shopify: false,
              custitem_rc_send_to_pfs: 2
            }
          });

          // Locations ONLY on create AND only if passed
          if (parentLocs && parentLocs.length) {
            try {
              var prodTmp = shopify.getProduct(pid);
              var defV = prodTmp && prodTmp.variants && prodTmp.variants[0] ? prodTmp.variants[0] : null;
              if (defV && defV.id) {
                shopify.setInventoryLocationsExact(String(defV.id), parentLocs);
                log.audit('FULL:parent-locations-set', { productId: pid, variantId: defV.id, locations: parentLocs });
              }
            } catch (eLocP) {
              log.error('FULL:parent-location-error', { productId: pid, err: eLocP });
            }
          }

          log.audit('FULL:parent-created', { itemId: row.itemId, productId: pid });
          return;
        }

        var parentProdCheck = getShopifyProductSafe(pid);
        if (!productExists(parentProdCheck)) {
          log.error('FULL:parent-skip-product-not-found', { itemId: row.itemId, productId: pid });
          return;
        }

        // UPDATE parent title (NO price)
        try { shopify.updateProductInfo(String(pid), String(row.displayName), undefined); } catch (e0) {}
        
        var parentLocsUpd = parseLocs(row.shopifyLocIds);
        if (parentLocsUpd && parentLocsUpd.length) {
          try {
            var prodUpd = shopify.getProduct(String(pid));
            var defVUpd = prodUpd && prodUpd.variants && prodUpd.variants[0] ? prodUpd.variants[0] : null;
            if (defVUpd && defVUpd.id) {
              shopify.syncInventoryLocationsMembership(String(defVUpd.id), parentLocsUpd);
              log.audit('FULL:parent-locations-synced', { productId: pid, variantId: defVUpd.id, locations: parentLocsUpd });
             }
            } catch (eLocPU) {
              log.error('FULL:parent-location-sync-error', { productId: pid, err: eLocPU });
            }
          }

        try {
          record.submitFields({
            type: shopify.getType(row.type),
            id: row.itemId,
            values: { custitem_rc_send_to_shopify: false }
          });
        } catch (e1) {}

        log.audit('FULL:parent-updated', { itemId: row.itemId, productId: pid });
        return;
      }

      // -------------------------
      // CHILD
      // -------------------------
      var parentPid = row.parentProductId || row.productId || null;

      if (!parentPid) {
        log.audit('FULL:child-skip-parent-not-ready', {
          itemId: row.itemId,
          parentItemId: row.parentItemId
        });
        return;
      }

      var childMetafields = buildMetafields(row.metafieldInput);
      var childLocs = parseLocs(row.shopifyLocIds);

      // Build option names/values
      var names = [];
      var vals  = [];
      var v1 = csvToArray(row.o1v)[0] || null;
      var v2 = csvToArray(row.o2v)[0] || null;
      var v3 = csvToArray(row.o3v)[0] || null;
      if (row.o1 && v1 != null) { names.push(row.o1); vals.push(v1); }
      if (row.o2 && v2 != null) { names.push(row.o2); vals.push(v2); }
      if (row.o3 && v3 != null) { names.push(row.o3); vals.push(v3); }

      var vid = row.variantId;

      // CREATE child variant (NO price)
      if (!vid) {
        var parentProdCheck2 = getShopifyProductSafe(parentPid);
        if (!productExists(parentProdCheck2)) {
          log.error('FULL:child-skip-parent-product-not-found', { itemId: row.itemId, parentProductId: parentPid });
          return;
        }

        var up = shopify.upsertVariantFromChild(
          String(parentPid),
          names,
          vals,
          row.sku,
          undefined, // price
          undefined, // compare_at
          childMetafields
        );

        if (!up || !up.variantId) {
          log.error('FULL:child-create-failed', { itemId: row.itemId, sku: row.sku, parentProductId: parentPid });
          return;
        }

        vid = String(up.variantId);

        var bcRes = shopify.updateVariantBarcode(String(vid), String(vid));
        if (row.costPerItem !== null && row.costPerItem !== undefined) {
          shopify.updateVariantCost(String(vid), row.costPerItem);
        }

        if (childLocs && childLocs.length) {
          try {
            shopify.setInventoryLocationsExact(String(vid), childLocs);
          } catch (eLocC) {}
        }

        record.submitFields({
          type: shopify.getType(row.type),
          id: row.itemId,
          values: {
            custitem_rc_shopify_product_id: String(parentPid),
            custitem_ring_shopify_item_id: String(vid),
            custitem_rc_send_to_shopify: false,
            custitem_rc_send_to_pfs: 2
          }
        });

        log.audit('FULL:child-created', { itemId: row.itemId, productId: parentPid, variantId: vid });
      } else {
        var parentProdCheck3 = getShopifyProductSafe(parentPid);
        if (!productExists(parentProdCheck3)) return;
        if (!variantExistsInProduct(parentProdCheck3, vid)) return;

        var editRes = shopify.editChildIfChanged(
          String(parentPid),
          String(vid),
          names,
          vals,
          row.sku,
          undefined,
          undefined,
          childMetafields
        );

        var finalVid = (editRes && editRes.variantId) ? String(editRes.variantId) : String(vid);
        if (row.costPerItem !== null && row.costPerItem !== undefined) {
          shopify.updateVariantCost(String(finalVid), row.costPerItem);
        }

        // ✅ Sync Barcode (as per your existing backup script logic)
        try {
          var desiredBarcode = (row.barcode || '').trim();
          if (desiredBarcode) {
            shopify.updateVariantBarcode(String(finalVid), desiredBarcode);
          }
        } catch (eBCU) {}

        // --- NEW: SYNC SHIPPING DATA ---
        if (finalVid && (row.countryOfOrigin || row.hsCode)) {
          try {
            shopify.updateInventoryShippingInfo(String(finalVid), {
              countryCodeOfOrigin: row.countryOfOrigin,
              harmonizedSystemCode: row.hsCode
            });
            log.audit('FULL:shipping-synced', { variantId: finalVid, country: row.countryOfOrigin, hts: row.hsCode });
          } catch (eShip) {
            log.error('FULL:shipping-sync-failed', eShip);
          }
        }

        var childLocsUpd = parseLocs(row.shopifyLocIds);
        if (childLocsUpd && childLocsUpd.length) {
          try {
            shopify.syncInventoryLocationsMembership(String(finalVid), childLocsUpd);
          } catch (eLocCU) {}
        }

        record.submitFields({
          type: shopify.getType(row.type),
          id: row.itemId,
          values: {
            custitem_rc_send_to_shopify: false,
            custitem_rc_send_to_pfs: 2
          }
        });

        log.audit('FULL:child-updated', { itemId: row.itemId, productId: parentPid, variantId: finalVid });
      }

    } catch (e) {
      log.error('FULLITEM:reduce-error', { key: key, err: e });
    }
  }

  function summarize(summary) {
    try {
      var done = {};
      var ok = 0, failed = 0;

      summary.output.iterator().each(function(key, value){
        if (key !== 'UNCK') return true;

        var obj = JSON.parse(value || '{}');
        var id = obj.itemId;
        var type = obj.type;
        if (!id || !type) return true;

        var uniq = String(type) + '|' + String(id);
        if (done[uniq]) return true;
        done[uniq] = true;

        try {
          record.submitFields({
            type: shopify.getType(type),
            id: id,
            values: {
              custitem_shopify_price_update: false
            }
          });
          ok++;
        } catch (e) {
          failed++;
          log.error('SUM:uncheck-failed', { itemId: id, type: type, err: e });
        }
        return true;
      });

      log.audit('SUM:uncheck-complete', { ok: ok, failed: failed });

    } catch (e2) {
      log.error('summarize-error', e2);
    }

    if (summary.inputSummary && summary.inputSummary.error) {
      log.error('Input Error', summary.inputSummary.error);
    }
    summary.mapSummary.errors.iterator().each(function(k, e){ log.error('Map Error ' + k, e); return true; });
    summary.reduceSummary.errors.iterator().each(function(k, e){ log.error('Reduce Error ' + k, e); return true; });
  }

  return { getInputData: getInputData, map: map, reduce: reduce, summarize: summarize  };
});