<?php
declare(strict_types=1);

/**
 * Bettavaro - Order Paid Handler
 *
 * Goals:
 * - Keep old paid-order flow working
 * - Add safe auction-paid flow without breaking fixed-price flow
 * - Queue notifications AFTER COMMIT
 * - Be idempotent
 * - Be discount-aware without recalculating paid totals after payment
 * - Capture refund fee snapshot at paid stage when refund fee engine exists
 */

if (is_file(__DIR__ . '/mailer.php')) {
    require_once __DIR__ . '/mailer.php';
}

if (is_file(__DIR__ . '/auction_engine.php')) {
    require_once __DIR__ . '/auction_engine.php';
}

if (is_file(__DIR__ . '/refund_fee_engine.php')) {
    require_once __DIR__ . '/refund_fee_engine.php';
}

function bvoph_h($v): string
{
    return htmlspecialchars((string)$v, ENT_QUOTES, 'UTF-8');
}

function bvoph_log(string $event, array $data = []): void
{
    $candidates = [
        dirname(__DIR__) . '/private_html/order_paid_handler.log',
        dirname(__DIR__) . '/order_paid_handler.log',
        __DIR__ . '/../order_paid_handler.log',
    ];

    $line = '[' . date('Y-m-d H:i:s') . '] ' . $event . ' ' . json_encode($data, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES) . PHP_EOL;

    foreach ($candidates as $file) {
        $dir = dirname($file);
        if (is_dir($dir) || @mkdir($dir, 0775, true)) {
            @file_put_contents($file, $line, FILE_APPEND);
            break;
        }
    }

    @error_log(trim($line));
}

function bvoph_table_exists(PDO $pdo, string $table): bool
{
    static $cache = [];
    if (isset($cache[$table])) {
        return $cache[$table];
    }

    try {
        $table = str_replace('`', '', $table);
        $stmt = $pdo->query("SHOW TABLES LIKE " . $pdo->quote($table));
        return $cache[$table] = (bool)$stmt->fetchColumn();
    } catch (Throwable $e) {
        return false;
    }
}

function bvoph_columns(PDO $pdo, string $table): array
{
    static $cache = [];
    if (isset($cache[$table])) {
        return $cache[$table];
    }

    $cols = [];
    try {
        $stmt = $pdo->query("SHOW COLUMNS FROM `{$table}`");
        while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
            if (!empty($row['Field'])) {
                $cols[$row['Field']] = true;
            }
        }
    } catch (Throwable $e) {
        // ignore
    }

    return $cache[$table] = $cols;
}

function bvoph_has_col(PDO $pdo, string $table, string $column): bool
{
    $cols = bvoph_columns($pdo, $table);
    return isset($cols[$column]);
}

function bvoph_now(): string
{
    return date('Y-m-d H:i:s');
}

function bvoph_num($value): float
{
    if ($value === null || $value === '') {
        return 0.0;
    }
    return (float)$value;
}

function bvoph_money(float $amount, string $currency = 'USD'): string
{
    $currency = strtoupper(trim($currency));
    if ($currency === '') {
        $currency = 'USD';
    }
    return $currency . ' ' . number_format($amount, 2);
}

function bvoph_refund_fee_engine_available(): bool
{
    return function_exists('bv_refund_fee_policy_detect_from_order')
        && function_exists('bv_refund_fee_policy_get')
        && function_exists('bv_refund_fee_build_order_paid_snapshot')
        && function_exists('bv_refund_fee_snapshot_save_to_order');
}

function bvoph_refund_fee_invoke_save(int $orderId, array $snapshot, PDO $pdo): bool
{
    if (!function_exists('bv_refund_fee_snapshot_save_to_order')) {
        return false;
    }

    try {
        bv_refund_fee_snapshot_save_to_order($orderId, $snapshot, $pdo);
        return true;
    } catch (ArgumentCountError $e) {
        try {
            bv_refund_fee_snapshot_save_to_order($orderId, $snapshot);
            return true;
        } catch (Throwable $inner) {
            bvoph_log('refund_fee_snapshot_save_failed', [
                'order_id' => $orderId,
                'error' => $inner->getMessage(),
            ]);
            return false;
        }
    } catch (Throwable $e) {
        bvoph_log('refund_fee_snapshot_save_failed', [
            'order_id' => $orderId,
            'error' => $e->getMessage(),
        ]);
        return false;
    }
}

function bvoph_capture_refund_fee_snapshot(PDO $pdo, array $order): array
{
    $empty = [
        'captured' => false,
        'snapshot' => [],
        'policy_code' => '',
        'error' => null,
    ];

    if (!bvoph_refund_fee_engine_available()) {
        return $empty;
    }

    $orderId = (int)($order['id'] ?? 0);
    if ($orderId <= 0) {
        return $empty;
    }

    try {
        $policyCode = (string)bv_refund_fee_policy_detect_from_order($order);
        if ($policyCode === '') {
            $policyCode = 'MARKETPLACE_STD';
        }

        $channel = trim((string)($order['order_source'] ?? $order['source'] ?? 'shop'));
        if ($channel === '') {
            $channel = 'shop';
        }

        $paymentProvider = trim((string)($order['payment_provider'] ?? ''));
        $policyRows = bv_refund_fee_policy_get($policyCode, $channel, $paymentProvider, 'both');
        $snapshot = bv_refund_fee_build_order_paid_snapshot($order, is_array($policyRows) ? $policyRows : []);

        if (!is_array($snapshot) || $snapshot === []) {
            return [
                'captured' => false,
                'snapshot' => [],
                'policy_code' => $policyCode,
                'error' => 'empty_snapshot',
            ];
        }

        if (!array_key_exists('fee_policy_code', $snapshot) || trim((string)$snapshot['fee_policy_code']) === '') {
            $snapshot['fee_policy_code'] = $policyCode;
        }

        if (!array_key_exists('fee_policy_snapshot', $snapshot) || trim((string)$snapshot['fee_policy_snapshot']) === '') {
            $snapshot['fee_policy_snapshot'] = json_encode([
                'policy_code' => $policyCode,
                'channel' => $channel,
                'payment_provider' => $paymentProvider,
                'captured_by' => 'order_paid_handler',
                'captured_at' => bvoph_now(),
            ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        }

        $grossPaid = isset($snapshot['gross_paid_amount']) ? (float)$snapshot['gross_paid_amount'] : bvoph_num($order['total'] ?? 0);
        $platformNonRefundable = isset($snapshot['platform_fee_non_refundable']) ? (float)$snapshot['platform_fee_non_refundable'] : 0.0;
        $gatewayNonRefundable = isset($snapshot['payment_gateway_fee_non_refundable']) ? (float)$snapshot['payment_gateway_fee_non_refundable'] : 0.0;

        if (!isset($snapshot['seller_net_amount_snapshot']) || !is_numeric($snapshot['seller_net_amount_snapshot'])) {
            $snapshot['seller_net_amount_snapshot'] = round(max(0, $grossPaid - $platformNonRefundable - $gatewayNonRefundable), 2);
        }

        $saved = bvoph_refund_fee_invoke_save($orderId, $snapshot, $pdo);

        return [
            'captured' => $saved,
            'snapshot' => $snapshot,
            'policy_code' => $policyCode,
            'error' => $saved ? null : 'save_failed',
        ];
    } catch (Throwable $e) {
        bvoph_log('refund_fee_snapshot_capture_failed', [
            'order_id' => $orderId,
            'error' => $e->getMessage(),
        ]);

        return [
            'captured' => false,
            'snapshot' => [],
            'policy_code' => '',
            'error' => $e->getMessage(),
        ];
    }
}

function bvoph_final_paid_order_status(PDO $pdo): string
{
    static $status = null;

    if ($status !== null) {
        return $status;
    }

    $status = 'confirmed';
    return $status;
}

function bvoph_fetch_order(PDO $pdo, int $orderId): ?array
{
    $stmt = $pdo->prepare("SELECT * FROM orders WHERE id = ? LIMIT 1");
    $stmt->execute([$orderId]);
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    return $row ?: null;
}

function bvoph_fetch_order_items(PDO $pdo, int $orderId): array
{
    if (!bvoph_table_exists($pdo, 'order_items')) {
        return [];
    }

    $stmt = $pdo->prepare("SELECT * FROM order_items WHERE order_id = ? ORDER BY id ASC");
    $stmt->execute([$orderId]);
    return $stmt->fetchAll(PDO::FETCH_ASSOC) ?: [];
}

function bvoph_fetch_listing_for_update(PDO $pdo, int $listingId): ?array
{
    $stmt = $pdo->prepare("SELECT * FROM listings WHERE id = ? LIMIT 1 FOR UPDATE");
    $stmt->execute([$listingId]);
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    return $row ?: null;
}

function bvoph_update_order(PDO $pdo, int $orderId, array $patch): void
{
    if (!$patch) {
        return;
    }

    $cols = bvoph_columns($pdo, 'orders');
    $sets = [];
    $params = [];

    foreach ($patch as $k => $v) {
        if (isset($cols[$k])) {
            $sets[] = "`{$k}` = ?";
            $params[] = $v;
        }
    }

    if (!$sets) {
        return;
    }

    if (isset($cols['updated_at']) && !array_key_exists('updated_at', $patch)) {
        $sets[] = "`updated_at` = ?";
        $params[] = bvoph_now();
    }

    $params[] = $orderId;
    $stmt = $pdo->prepare("UPDATE orders SET " . implode(', ', $sets) . " WHERE id = ?");
    $stmt->execute($params);
}

function bvoph_extract_discount_snapshot(array $order): array
{
    $currency = strtoupper(trim((string)($order['currency'] ?? 'USD')));
    if ($currency === '') {
        $currency = 'USD';
    }

    $subtotalBeforeDiscount = bvoph_num($order['subtotal_before_discount'] ?? 0);
    $discountAmount         = bvoph_num($order['discount_amount'] ?? 0);
    $sellerDiscountTotal    = bvoph_num($order['seller_discount_total'] ?? 0);
    $subtotal               = bvoph_num($order['subtotal'] ?? 0);
    $shippingAmount         = bvoph_num($order['shipping_amount'] ?? 0);
    $total                  = bvoph_num($order['total'] ?? 0);

    if ($subtotalBeforeDiscount <= 0 && $subtotal > 0 && $discountAmount > 0) {
        $subtotalBeforeDiscount = $subtotal + $discountAmount;
    }

    if ($discountAmount <= 0 && $subtotalBeforeDiscount > 0 && $subtotal > 0 && $subtotalBeforeDiscount >= $subtotal) {
        $discountAmount = $subtotalBeforeDiscount - $subtotal;
    }

    if ($sellerDiscountTotal <= 0 && $discountAmount > 0) {
        $sellerDiscountTotal = $discountAmount;
    }

    $subtotalAfterDiscount = $subtotal;
    if ($subtotalAfterDiscount <= 0 && $subtotalBeforeDiscount > 0) {
        $subtotalAfterDiscount = max(0, $subtotalBeforeDiscount - $discountAmount);
    }

    $computedTotal = $subtotalAfterDiscount + $shippingAmount;
    if ($total <= 0 && $computedTotal > 0) {
        $total = $computedTotal;
    }

    $commissionBase = $subtotalAfterDiscount;
    if ($commissionBase <= 0 && $subtotalBeforeDiscount > 0) {
        $commissionBase = max(0, $subtotalBeforeDiscount - $sellerDiscountTotal);
    }
    if ($commissionBase <= 0 && $total > 0) {
        $commissionBase = max(0, $total - $shippingAmount);
    }

    return [
        'currency'                 => $currency,
        'subtotal_before_discount' => round($subtotalBeforeDiscount, 2),
        'discount_amount'          => round($discountAmount, 2),
        'seller_discount_total'    => round($sellerDiscountTotal, 2),
        'subtotal_after_discount'  => round($subtotalAfterDiscount, 2),
        'shipping_amount'          => round($shippingAmount, 2),
        'total'                    => round($total, 2),
        'commission_base'          => round(max(0, $commissionBase), 2),
        'has_discount'             => ($discountAmount > 0.00001 || $sellerDiscountTotal > 0.00001),
    ];
}

function bvoph_discount_summary_text(array $discount): string
{
    $currency = (string)($discount['currency'] ?? 'USD');
    $parts = [
        'Before discount: ' . bvoph_money((float)($discount['subtotal_before_discount'] ?? 0), $currency),
        'Discount: ' . bvoph_money((float)($discount['discount_amount'] ?? 0), $currency),
        'Seller discount total: ' . bvoph_money((float)($discount['seller_discount_total'] ?? 0), $currency),
        'Subtotal after discount: ' . bvoph_money((float)($discount['subtotal_after_discount'] ?? 0), $currency),
        'Shipping: ' . bvoph_money((float)($discount['shipping_amount'] ?? 0), $currency),
        'Total paid: ' . bvoph_money((float)($discount['total'] ?? 0), $currency),
        'Commission base: ' . bvoph_money((float)($discount['commission_base'] ?? 0), $currency),
    ];

    return implode(' | ', $parts);
}

function bvoph_build_stock_log_payload(
    int $orderId,
    int $itemId,
    int $listingId,
    array $listing,
    array $res,
    int $qty,
    string $source,
    string $finalOrderStatus
): array {
    return [
        'event_key'              => 'order_paid:' . $orderId . ':item:' . $itemId . ':listing:' . $listingId,
        'source'                 => $source,
        'order_id'               => $orderId,
        'listing_id'             => $listingId,
        'order_status'           => $finalOrderStatus,
        'payment_status'         => 'paid',
        'qty_paid'               => $qty,
        'qty_reserved'           => 0,
        'stock_total'            => (int)($res['after_stock_total'] ?? 0),
        'stock_sold_before'      => (int)($res['before_stock_sold'] ?? 0),
        'stock_sold_after'       => (int)($res['after_stock_sold'] ?? 0),
        'stock_available_before' => (int)($res['before_stock_available'] ?? 0),
        'stock_available_after'  => (int)($res['after_stock_available'] ?? 0),
        'sale_status_before'     => (string)($res['before_sale_status'] ?? ''),
        'sale_status_after'      => (string)($res['after_sale_status'] ?? ''),
        'status_before'          => (string)($listing['status'] ?? ''),
        'status_after'           => (string)($res['after_status'] ?? ''),
        'details_json'           => json_encode([
            'event'         => 'cut_stock_after_paid',
            'order_item_id' => $itemId,
            'qty'           => $qty,
            'message'       => 'Stock synced after paid handler',
            'handler'       => 'bv_handle_paid_order',
        ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES),
    ];
}

function bvoph_insert_stock_log(PDO $pdo, array $data): void
{
    if (!bvoph_table_exists($pdo, 'order_stock_sync_logs')) {
        return;
    }

    $cols = bvoph_columns($pdo, 'order_stock_sync_logs');

    if (isset($cols['event_key']) && empty($data['event_key'])) {
        $data['event_key'] = 'stocklog:' . md5(json_encode([
            $data['order_id'] ?? null,
            $data['order_item_id'] ?? null,
            $data['listing_id'] ?? null,
            $data['action'] ?? null,
            $data['qty'] ?? null,
            date('Y-m-d H:i:s'),
        ]));
    }

    $insertCols = [];
    $insertVals = [];
    $params = [];

    foreach ($data as $k => $v) {
        if (isset($cols[$k])) {
            $insertCols[] = "`{$k}`";
            $insertVals[] = "?";
            $params[] = $v;
        }
    }

    if (isset($cols['created_at'])) {
        $insertCols[] = "`created_at`";
        $insertVals[] = "?";
        $params[] = bvoph_now();
    }

    if (!$insertCols) {
        return;
    }

    $stmt = $pdo->prepare(
        "INSERT INTO order_stock_sync_logs (" . implode(', ', $insertCols) . ")
         VALUES (" . implode(', ', $insertVals) . ")"
    );
    $stmt->execute($params);
}

function bvoph_insert_audit_log(PDO $pdo, array $data): void
{
    if (!bvoph_table_exists($pdo, 'audit_logs')) {
        return;
    }

    $cols = bvoph_columns($pdo, 'audit_logs');
    $payload = [
        'actor_type'   => $data['actor_type'] ?? 'system',
        'actor_id'     => $data['actor_id'] ?? null,
        'actor_name'   => $data['actor_name'] ?? null,
        'actor_email'  => $data['actor_email'] ?? null,
        'event_type'   => $data['event_type'] ?? null,
        'entity_type'  => $data['entity_type'] ?? 'order',
        'entity_id'    => $data['entity_id'] ?? null,
        'entity_title' => $data['entity_title'] ?? null,
        'action'       => $data['action'] ?? null,
        'summary'      => $data['summary'] ?? null,
        'before_json'  => isset($data['before_json']) ? json_encode($data['before_json'], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES) : null,
        'after_json'   => isset($data['after_json']) ? json_encode($data['after_json'], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES) : null,
        'created_at'   => $data['created_at'] ?? bvoph_now(),
    ];

    $insertCols = [];
    $insertVals = [];
    $params = [];

    foreach ($payload as $k => $v) {
        if (isset($cols[$k])) {
            $insertCols[] = "`{$k}`";
            $insertVals[] = "?";
            $params[] = $v;
        }
    }

    if (!$insertCols) {
        return;
    }

    $stmt = $pdo->prepare(
        "INSERT INTO audit_logs (" . implode(', ', $insertCols) . ")
         VALUES (" . implode(', ', $insertVals) . ")"
    );
    $stmt->execute($params);
}

function bvoph_insert_discount_log(PDO $pdo, array $order, array $discount, string $source, string $status): void
{
    $tables = ['order_discount_log', 'order_discount_logs'];

    foreach ($tables as $table) {
        if (!bvoph_table_exists($pdo, $table)) {
            continue;
        }

        $cols = bvoph_columns($pdo, $table);
        if (!$cols) {
            continue;
        }

        $row = [
            'order_id'                 => (int)($order['id'] ?? 0),
            'order_code'               => (string)($order['order_code'] ?? ''),
            'event_type'               => 'order_paid',
            'action'                   => 'paid_confirmed',
            'status'                   => $status,
            'source'                   => $source,
            'currency'                 => (string)($discount['currency'] ?? 'USD'),
            'subtotal_before_discount' => (float)($discount['subtotal_before_discount'] ?? 0),
            'discount_amount'          => (float)($discount['discount_amount'] ?? 0),
            'seller_discount_total'    => (float)($discount['seller_discount_total'] ?? 0),
            'subtotal_after_discount'  => (float)($discount['subtotal_after_discount'] ?? 0),
            'shipping_amount'          => (float)($discount['shipping_amount'] ?? 0),
            'total'                    => (float)($discount['total'] ?? 0),
            'commission_base'          => (float)($discount['commission_base'] ?? 0),
            'details_json'             => json_encode([
                'message' => 'Discount snapshot captured by order_paid_handler',
                'source'  => $source,
                'status'  => $status,
            ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES),
            'note'                     => 'Discount snapshot captured on paid flow',
            'created_at'               => bvoph_now(),
            'updated_at'               => bvoph_now(),
        ];

        $insertCols = [];
        $insertVals = [];
        $params = [];

        foreach ($row as $k => $v) {
            if (isset($cols[$k])) {
                $insertCols[] = "`{$k}`";
                $insertVals[] = "?";
                $params[] = $v;
            }
        }

        if (!$insertCols) {
            return;
        }

        $stmt = $pdo->prepare(
            "INSERT INTO `{$table}` (" . implode(', ', $insertCols) . ")
             VALUES (" . implode(', ', $insertVals) . ")"
        );
        $stmt->execute($params);
        return;
    }
}

function bvoph_find_listing_seller_email(PDO $pdo, int $listingId): string
{
    try {
        $stmt = $pdo->prepare("
            SELECT u.email
            FROM listings l
            LEFT JOIN users u ON u.id = l.seller_id
            WHERE l.id = ?
            LIMIT 1
        ");
        $stmt->execute([$listingId]);
        return trim((string)$stmt->fetchColumn());
    } catch (Throwable $e) {
        return '';
    }
}

function bvoph_is_auction_item(array $item, array $order, array $listing): bool
{
    $itemSource = strtolower(trim((string)($item['source'] ?? $item['order_source'] ?? '')));
    $orderSource = strtolower(trim((string)($order['order_source'] ?? $order['source'] ?? '')));
    $listingSaleFormat = strtolower(trim((string)($listing['sale_format'] ?? '')));

    if ($itemSource === 'auction' || $orderSource === 'auction' || $listingSaleFormat === 'auction') {
        return true;
    }

    if (!empty($listing['auction_winner_user_id']) || !empty($listing['winner_payment_due_at'])) {
        return true;
    }

    return false;
}

function bvoph_update_listing_stock(PDO $pdo, int $listingId, array $listing, int $qty): array
{
    $beforeTotal      = (int)($listing['stock_total'] ?? 0);
    $beforeSold       = (int)($listing['stock_sold'] ?? 0);
    $beforeAvailable  = (int)($listing['stock_available'] ?? max(0, $beforeTotal - $beforeSold));
    $beforeSaleStatus = (string)($listing['sale_status'] ?? 'available');

    $afterSold       = $beforeSold + $qty;
    $afterAvailable  = max(0, $beforeTotal - $afterSold);
    $afterSaleStatus = $afterAvailable > 0 ? 'available' : 'sold';
    $afterStatus     = $afterAvailable > 0 ? 'active' : 'sold';

    $patch = [];

    if (bvoph_has_col($pdo, 'listings', 'stock_sold')) {
        $patch['stock_sold'] = $afterSold;
    }
    if (bvoph_has_col($pdo, 'listings', 'stock_available')) {
        $patch['stock_available'] = $afterAvailable;
    }
    if (bvoph_has_col($pdo, 'listings', 'sale_status')) {
        $patch['sale_status'] = $afterSaleStatus;
    }
    if (bvoph_has_col($pdo, 'listings', 'status')) {
        $patch['status'] = $afterStatus;
    }
    if ($afterAvailable <= 0 && bvoph_has_col($pdo, 'listings', 'sold_at')) {
        $patch['sold_at'] = bvoph_now();
    }

    if ($patch) {
        $sets = [];
        $params = [];
        foreach ($patch as $k => $v) {
            $sets[] = "`{$k}` = ?";
            $params[] = $v;
        }
        if (bvoph_has_col($pdo, 'listings', 'updated_at')) {
            $sets[] = "`updated_at` = ?";
            $params[] = bvoph_now();
        }
        $params[] = $listingId;

        $stmt = $pdo->prepare("UPDATE listings SET " . implode(', ', $sets) . " WHERE id = ?");
        $stmt->execute($params);
    }

    return [
        'before_stock_total'     => $beforeTotal,
        'before_stock_sold'      => $beforeSold,
        'before_stock_available' => $beforeAvailable,
        'before_sale_status'     => $beforeSaleStatus,
        'after_stock_total'      => $beforeTotal,
        'after_stock_sold'       => $afterSold,
        'after_stock_available'  => $afterAvailable,
        'after_sale_status'      => $afterSaleStatus,
        'after_status'           => $afterStatus,
    ];
}

function bvoph_update_listing_after_auction_paid(PDO $pdo, int $listingId, array $listing, array $item, int $qty): array
{
    $beforeTotal         = (int)($listing['stock_total'] ?? 0);
    $beforeSold          = (int)($listing['stock_sold'] ?? 0);
    $beforeAvailable     = (int)($listing['stock_available'] ?? max(0, $beforeTotal - $beforeSold));
    $beforeSaleStatus    = (string)($listing['sale_status'] ?? 'reserved');
    $beforeAuctionStatus = (string)($listing['auction_status'] ?? '');
    $beforeStatus        = (string)($listing['status'] ?? '');

    $afterSold       = $beforeSold;
    $afterAvailable  = $beforeAvailable;

    if ($beforeTotal > 0) {
        $afterSold = min($beforeTotal, max($beforeSold, $qty));
        $afterAvailable = max(0, $beforeTotal - $afterSold);
    } elseif ($beforeAvailable > 0) {
        $afterAvailable = max(0, $beforeAvailable - $qty);
        $afterSold = $beforeSold + $qty;
    }

    $patch = [];

    if (bvoph_has_col($pdo, 'listings', 'stock_sold')) {
        $patch['stock_sold'] = $afterSold;
    }
    if (bvoph_has_col($pdo, 'listings', 'stock_available')) {
        $patch['stock_available'] = $afterAvailable;
    }
    if (bvoph_has_col($pdo, 'listings', 'sale_status')) {
        $patch['sale_status'] = 'sold';
    }
    if (bvoph_has_col($pdo, 'listings', 'auction_status')) {
        $patch['auction_status'] = 'paid';
    }
    if (bvoph_has_col($pdo, 'listings', 'status')) {
        $patch['status'] = 'sold';
    }
    if (bvoph_has_col($pdo, 'listings', 'sold_at')) {
        $patch['sold_at'] = bvoph_now();
    }
    if (bvoph_has_col($pdo, 'listings', 'winner_payment_due_at')) {
        $patch['winner_payment_due_at'] = null;
    }
    if (bvoph_has_col($pdo, 'listings', 'updated_at')) {
        $patch['updated_at'] = bvoph_now();
    }

    if ($patch) {
        $sets = [];
        $params = [];
        foreach ($patch as $k => $v) {
            $sets[] = "`{$k}` = ?";
            $params[] = $v;
        }
        $params[] = $listingId;

        $stmt = $pdo->prepare("UPDATE listings SET " . implode(', ', $sets) . " WHERE id = ?");
        $stmt->execute($params);
    }

    return [
        'before_stock_total'     => $beforeTotal,
        'before_stock_sold'      => $beforeSold,
        'before_stock_available' => $beforeAvailable,
        'before_sale_status'     => $beforeSaleStatus,
        'after_stock_total'      => $beforeTotal,
        'after_stock_sold'       => $afterSold,
        'after_stock_available'  => $afterAvailable,
        'after_sale_status'      => 'sold',
        'after_status'           => 'sold',
        'before_auction_status'  => $beforeAuctionStatus,
        'after_auction_status'   => 'paid',
        'before_status'          => $beforeStatus,
    ];
}

function bvoph_update_auction_award_paid(PDO $pdo, int $listingId, int $orderId): void
{
    $tables = ['auction_awards', 'listing_auction_awards'];

    foreach ($tables as $table) {
        if (!bvoph_table_exists($pdo, $table)) {
            continue;
        }

        $cols = bvoph_columns($pdo, $table);
        $sets = [];
        $params = [];

        if (isset($cols['award_status'])) {
            $sets[] = "`award_status` = ?";
            $params[] = 'paid';
        } elseif (isset($cols['awarded_status'])) {
            $sets[] = "`awarded_status` = ?";
            $params[] = 'paid';
        }

        if (isset($cols['paid_at'])) {
            $sets[] = "`paid_at` = ?";
            $params[] = bvoph_now();
        }

        if (isset($cols['order_id'])) {
            $sets[] = "`order_id` = ?";
            $params[] = $orderId;
        }

        if (!$sets) {
            continue;
        }

        $params[] = $listingId;
        $stmt = $pdo->prepare("UPDATE `{$table}` SET " . implode(', ', $sets) . " WHERE `listing_id` = ?");
        $stmt->execute($params);
    }
}

function bvoph_validate_item_for_paid(PDO $pdo, array $order, array $item, array $listing, int $qty): array
{
    $listingId = (int)($listing['id'] ?? 0);
    $saleStatus = (string)($listing['sale_status'] ?? 'available');
    $listingStatus = (string)($listing['status'] ?? 'active');
    $stockAvailable = (int)($listing['stock_available'] ?? 0);
    $isAuction = bvoph_is_auction_item($item, $order, $listing);

    if ($qty <= 0) {
        return [
            'ok' => false,
            'mode' => $isAuction ? 'auction' : 'fixed',
            'error' => "order_item " . (int)($item['id'] ?? 0) . ": invalid quantity",
        ];
    }

    if ($isAuction) {
        $auctionStatus = strtolower(trim((string)($listing['auction_status'] ?? '')));
        $winnerUserId = (int)($listing['auction_winner_user_id'] ?? 0);
        $orderUserId = (int)($order['user_id'] ?? 0);

        if ($winnerUserId > 0 && $orderUserId > 0 && $winnerUserId !== $orderUserId) {
            return [
                'ok' => false,
                'mode' => 'auction',
                'error' => "listing {$listingId}: order user does not match auction winner",
            ];
        }

        if (!in_array($saleStatus, ['reserved', 'sold'], true)) {
            return [
                'ok' => false,
                'mode' => 'auction',
                'error' => "listing {$listingId}: auction sale_status={$saleStatus}",
            ];
        }

        if (!in_array($auctionStatus, ['awaiting_payment', 'paid'], true)) {
            return [
                'ok' => false,
                'mode' => 'auction',
                'error' => "listing {$listingId}: auction_status={$auctionStatus}",
            ];
        }

        return [
            'ok' => true,
            'mode' => 'auction',
            'error' => null,
        ];
    }

    if ($saleStatus !== 'available') {
        return [
            'ok' => false,
            'mode' => 'fixed',
            'error' => "listing {$listingId}: sale_status={$saleStatus}",
        ];
    }

    if (!in_array($listingStatus, ['active'], true)) {
        return [
            'ok' => false,
            'mode' => 'fixed',
            'error' => "listing {$listingId}: status={$listingStatus}",
        ];
    }

    if ($stockAvailable < $qty) {
        return [
            'ok' => false,
            'mode' => 'fixed',
            'error' => "listing {$listingId}: stock_available={$stockAvailable}, need={$qty}",
        ];
    }

    return [
        'ok' => true,
        'mode' => 'fixed',
        'error' => null,
    ];
}

function bvoph_build_notification_payload(PDO $pdo, int $orderId): array
{
    $order = bvoph_fetch_order($pdo, $orderId);
    if (!$order) {
        return [];
    }

    $items = bvoph_fetch_order_items($pdo, $orderId);
    $discount = bvoph_extract_discount_snapshot($order);

    $buyerEmail = trim((string)($order['buyer_email'] ?? $order['ship_email'] ?? ''));
    $buyerName  = trim((string)($order['buyer_name'] ?? $order['ship_name'] ?? 'Customer'));
    $sellerEmails = [];
    $listingTitles = [];

    foreach ($items as $item) {
        $sellerEmail = trim((string)($item['seller_email'] ?? ''));
        if ($sellerEmail === '') {
            $listingId = (int)($item['listing_id'] ?? 0);
            if ($listingId > 0) {
                $sellerEmail = bvoph_find_listing_seller_email($pdo, $listingId);
            }
        }
        if ($sellerEmail !== '') {
            $sellerEmails[$sellerEmail] = true;
        }

        $title = trim((string)($item['title'] ?? $item['item_name'] ?? $item['listing_title'] ?? ''));
        if ($title !== '') {
            $listingTitles[$title] = true;
        }
    }

    $adminEmail = function_exists('bv_mailer_role_email')
        ? trim((string)bv_mailer_role_email('admin_alert', ''))
        : '';

    return [
        'order_id'         => $orderId,
        'order_code'       => $order['order_code'] ?? ('#' . $orderId),
        'buyer_email'      => $buyerEmail,
        'buyer_name'       => $buyerName,
        'seller_emails'    => array_keys($sellerEmails),
        'listing_titles'   => array_keys($listingTitles),
        'admin_email'      => $adminEmail,
        'source'           => $order['confirmed_by_source'] ?? null,
        'discount'         => $discount,
        'discount_summary' => bvoph_discount_summary_text($discount),
    ];
}

function bvoph_queue_notifications(PDO $pdo, array $payload): array
{
    $result = [
        'buyer' => null,
        'admin' => null,
        'sellers' => [],
    ];

    if (!function_exists('bv_queue_mail')) {
        return ['queued' => false, 'reason' => 'queue_not_available', 'result' => $result];
    }

    $orderId          = (int)($payload['order_id'] ?? 0);
    $orderCode        = (string)($payload['order_code'] ?? ('#' . $orderId));
    $buyerEmail       = trim((string)($payload['buyer_email'] ?? ''));
    $buyerName        = trim((string)($payload['buyer_name'] ?? 'Customer'));
    $adminEmail       = trim((string)($payload['admin_email'] ?? ''));
    $listingText      = trim(implode(', ', (array)($payload['listing_titles'] ?? [])));
    $discount         = (array)($payload['discount'] ?? []);
    $discountSummary  = (string)($payload['discount_summary'] ?? '');

    $buyerDiscountHtml = '';
    $buyerDiscountText = '';
    if (!empty($discount['has_discount'])) {
        $buyerDiscountHtml =
            '<p><strong>Order totals</strong><br>' .
            'Subtotal before discount: ' . bvoph_h(bvoph_money((float)$discount['subtotal_before_discount'], (string)$discount['currency'])) . '<br>' .
            'Discount: ' . bvoph_h(bvoph_money((float)$discount['discount_amount'], (string)$discount['currency'])) . '<br>' .
            'Total paid: ' . bvoph_h(bvoph_money((float)$discount['total'], (string)$discount['currency'])) .
            '</p>';
        $buyerDiscountText = ' | ' . $discountSummary;
    }

    if ($buyerEmail !== '') {
        $result['buyer'] = bv_queue_mail([
            'queue_key' => 'order_paid_buyer_' . $orderId,
            'profile'   => 'default',
            'to'        => [$buyerEmail],
            'subject'   => '[Bettavaro] Payment confirmed for order ' . $orderCode,
            'html'      => '<p>Hello ' . bvoph_h($buyerName) . ',</p>'
                . '<p>Your payment has been confirmed for order <strong>' . bvoph_h($orderCode) . '</strong>.</p>'
                . ($listingText !== '' ? '<p><strong>Items:</strong> ' . bvoph_h($listingText) . '</p>' : '')
                . $buyerDiscountHtml
                . '<p>We have started processing your order.</p>',
            'text'      => 'Your payment has been confirmed for order ' . $orderCode . ($listingText !== '' ? ' | Items: ' . $listingText : '') . $buyerDiscountText,
            'meta'      => [
                'event_type'      => 'order_paid',
                'target'          => 'buyer',
                'order_id'        => $orderId,
                'source'          => 'order_paid_handler',
                'commission_base' => $discount['commission_base'] ?? null,
            ],
        ]);
    }

    foreach ((array)($payload['seller_emails'] ?? []) as $sellerEmail) {
        $sellerEmail = trim((string)$sellerEmail);
        if ($sellerEmail === '') {
            continue;
        }

        $result['sellers'][$sellerEmail] = bv_queue_mail([
            'queue_key' => 'order_paid_seller_' . $orderId . '_' . md5(strtolower($sellerEmail)),
            'profile'   => 'support',
            'to'        => [$sellerEmail],
            'subject'   => '[Bettavaro] Order paid: ' . $orderCode,
            'html'      => '<p>Hello Seller,</p>'
                . '<p>An order containing your listing has been paid and confirmed.</p>'
                . '<p><strong>Order:</strong> ' . bvoph_h($orderCode) . '</p>'
                . ($listingText !== '' ? '<p><strong>Items:</strong> ' . bvoph_h($listingText) . '</p>' : '')
                . '<p>Please prepare the next fulfillment step.</p>',
            'text'      => 'An order containing your listing has been paid and confirmed. Order: ' . $orderCode . ($listingText !== '' ? ' | Items: ' . $listingText : ''),
            'meta'      => [
                'event_type'      => 'order_paid',
                'target'          => 'seller',
                'order_id'        => $orderId,
                'source'          => 'order_paid_handler',
                'commission_base' => $discount['commission_base'] ?? null,
            ],
        ]);
    }

    if ($adminEmail !== '') {
        $result['admin'] = bv_queue_mail([
            'queue_key' => 'order_paid_admin_' . $orderId,
            'profile'   => 'support',
            'to'        => [$adminEmail],
            'subject'   => '[Bettavaro] Order paid ' . $orderCode,
            'html'      => '<p>Admin alert: payment confirmed.</p>'
                . '<p><strong>Order:</strong> ' . bvoph_h($orderCode) . '</p>'
                . ($listingText !== '' ? '<p><strong>Items:</strong> ' . bvoph_h($listingText) . '</p>' : '')
                . '<p><strong>Source:</strong> ' . bvoph_h((string)($payload['source'] ?? 'system')) . '</p>'
                . '<p><strong>Discount snapshot:</strong><br>' . nl2br(bvoph_h(str_replace(' | ', PHP_EOL, $discountSummary))) . '</p>',
            'text'      => 'Admin alert: payment confirmed for order ' . $orderCode . ' | ' . $discountSummary,
            'meta'      => [
                'event_type'               => 'order_paid',
                'target'                   => 'admin',
                'order_id'                 => $orderId,
                'source'                   => 'order_paid_handler',
                'subtotal_before_discount' => $discount['subtotal_before_discount'] ?? null,
                'discount_amount'          => $discount['discount_amount'] ?? null,
                'seller_discount_total'    => $discount['seller_discount_total'] ?? null,
                'commission_base'          => $discount['commission_base'] ?? null,
            ],
        ]);
    }

    bvoph_log('notifications_queued', [
        'order_id' => $orderId,
        'result'   => $result,
        'source'   => $payload['source'] ?? null,
        'discount' => $discount,
    ]);

    return ['queued' => true, 'result' => $result];
}

function bv_order_paid_handler_after_commit(PDO $pdo, array $afterCommit): array
{
    $result = ['notifications_queued' => null];

    if (!empty($afterCommit['queue_notifications']) && !empty($afterCommit['notification_payload'])) {
        $result['notifications_queued'] = bvoph_queue_notifications($pdo, $afterCommit['notification_payload']);
    }

    return $result;
}

function bv_handle_paid_order(PDO $pdo, int $orderId, array $context = []): array
{
    $source     = (string)($context['source'] ?? 'system');
    $actorType  = (string)($context['actor_type'] ?? 'system');
    $actorId    = $context['actor_id'] ?? null;
    $actorName  = (string)($context['actor_name'] ?? ucfirst(str_replace('_', ' ', $source)));
    $actorEmail = $context['actor_email'] ?? null;

    $startedTx = false;
    $afterCommit = [];

    try {
        if (!$pdo->inTransaction()) {
            $pdo->beginTransaction();
            $startedTx = true;
        }

        $order = bvoph_fetch_order($pdo, $orderId);
        if (!$order) {
            throw new RuntimeException("Order not found: {$orderId}");
        }

        $orderCode = (string)($order['order_code'] ?? ('#' . $orderId));
        $discount = bvoph_extract_discount_snapshot($order);
        $feeSnapshotResult = bvoph_capture_refund_fee_snapshot($pdo, $order);

        if (!empty($order['paid_handler_done_at'])) {
            $currentStatus = (string)($order['status'] ?? '');

            if ($startedTx && $pdo->inTransaction()) {
                $pdo->commit();
            }

            bvoph_log('paid_handler_skip_already_done', [
                'order_id' => $orderId,
                'status' => $currentStatus,
                'paid_handler_done_at' => (string)($order['paid_handler_done_at'] ?? ''),
                'source' => $source,
                'discount' => $discount,
                'fee_snapshot' => $feeSnapshotResult,
            ]);

            return [
                'ok' => true,
                'order_id' => $orderId,
                'status' => $currentStatus,
                'already_done' => true,
                'auto_confirmed' => in_array($currentStatus, ['confirmed', 'processing', 'completed'], true),
                'after_commit' => [
                    'queue_notifications' => false,
                    'notification_payload' => [],
                ],
            ];
        }

        $items = bvoph_fetch_order_items($pdo, $orderId);
        if (!$items) {
            throw new RuntimeException("No order_items found for order {$orderId}");
        }

        $validationErrors = [];
        $stockChanges = [];

        foreach ($items as $item) {
            $itemId    = (int)($item['id'] ?? 0);
            $listingId = (int)($item['listing_id'] ?? 0);
            $qty       = (int)($item['quantity'] ?? $item['qty'] ?? 0);

            if ($listingId <= 0) {
                $validationErrors[] = "order_item {$itemId}: missing listing_id";
                continue;
            }

            $listing = bvoph_fetch_listing_for_update($pdo, $listingId);
            if (!$listing) {
                $validationErrors[] = "listing {$listingId}: not found";
                continue;
            }

            $check = bvoph_validate_item_for_paid($pdo, $order, $item, $listing, $qty);
            if (empty($check['ok'])) {
                $validationErrors[] = (string)$check['error'];
                continue;
            }

            $stockChanges[] = [
                'item'    => $item,
                'listing' => $listing,
                'qty'     => $qty,
                'mode'    => (string)($check['mode'] ?? 'fixed'),
            ];
        }

        $now = bvoph_now();
        $paymentRef = (string)($order['payment_ref'] ?? $order['stripe_payment_intent_id'] ?? $order['stripe_session_id'] ?? '');

        if ($validationErrors) {
            $patch = ['status' => 'paid'];
            if (bvoph_has_col($pdo, 'orders', 'confirmed_by_source')) {
                $patch['confirmed_by_source'] = null;
            }
            if (bvoph_has_col($pdo, 'orders', 'payment_status')) {
                $patch['payment_status'] = 'paid';
            }

            bvoph_update_order($pdo, $orderId, $patch);

            bvoph_insert_discount_log($pdo, $order, $discount, $source, 'paid_validation_blocked');

            bvoph_insert_audit_log($pdo, [
                'actor_type'   => $actorType,
                'actor_id'     => $actorId,
                'actor_name'   => $actorName,
                'actor_email'  => $actorEmail,
                'event_type'   => 'order.paid_validation_blocked',
                'entity_type'  => 'order',
                'entity_id'    => $orderId,
                'entity_title' => $orderCode,
                'action'       => 'paid_kept_without_confirm',
                'summary'      => 'Order paid but auto confirm blocked: ' . implode(' | ', $validationErrors) . ' | ' . bvoph_discount_summary_text($discount),
                'before_json'  => [
                    'status' => $order['status'] ?? null,
                    'paid_handler_done_at' => $order['paid_handler_done_at'] ?? null,
                ],
                'after_json'   => [
                    'status' => 'paid',
                    'errors' => $validationErrors,
                    'payment_ref' => $paymentRef,
                    'paid_handler_done_at' => null,
                    'discount' => $discount,
                    'fee_snapshot' => $feeSnapshotResult,
                ],
            ]);

            bvoph_log('paid_handler_validation_blocked', [
                'order_id' => $orderId,
                'order_code' => $orderCode,
                'source' => $source,
                'order_status' => 'paid',
                'payment_status' => 'paid',
                'errors' => $validationErrors,
                'discount' => $discount,
                'fee_snapshot' => $feeSnapshotResult,
            ]);

            if ($startedTx && $pdo->inTransaction()) {
                $pdo->commit();
            }

            return [
                'ok' => true,
                'order_id' => $orderId,
                'status' => 'paid',
                'already_done' => false,
                'auto_confirmed' => false,
                'errors' => $validationErrors,
                'after_commit' => [
                    'queue_notifications' => false,
                    'notification_payload' => [],
                ],
            ];
        }

        $finalOrderStatus = bvoph_final_paid_order_status($pdo);

        foreach ($stockChanges as $row) {
            $item = $row['item'];
            $listing = $row['listing'];
            $qty = (int)$row['qty'];
            $mode = (string)$row['mode'];

            $listingId = (int)($listing['id'] ?? 0);
            $itemId = (int)($item['id'] ?? 0);

            if ($mode === 'auction') {
                $res = bvoph_update_listing_after_auction_paid($pdo, $listingId, $listing, $item, $qty);
                bvoph_update_auction_award_paid($pdo, $listingId, $orderId);
            } else {
                $res = bvoph_update_listing_stock($pdo, $listingId, $listing, $qty);
            }

            bvoph_insert_stock_log($pdo, bvoph_build_stock_log_payload(
                $orderId,
                $itemId,
                $listingId,
                $listing,
                $res,
                $qty,
                $source,
                $finalOrderStatus
            ));
        }

        $patch = [
            'status' => $finalOrderStatus,
            'paid_handler_done_at' => $now,
        ];

        if (bvoph_has_col($pdo, 'orders', 'payment_status')) {
            $patch['payment_status'] = 'paid';
        }
        if ($finalOrderStatus === 'confirmed' && bvoph_has_col($pdo, 'orders', 'confirmed_at')) {
            $patch['confirmed_at'] = $now;
        }
        if (bvoph_has_col($pdo, 'orders', 'confirmed_by_source')) {
            $patch['confirmed_by_source'] = $source;
        }

        bvoph_update_order($pdo, $orderId, $patch);

        $order = bvoph_fetch_order($pdo, $orderId) ?? $order;
        $feeSnapshotResult = bvoph_capture_refund_fee_snapshot($pdo, $order);

        bvoph_insert_discount_log($pdo, $order, $discount, $source, $finalOrderStatus);

        bvoph_insert_audit_log($pdo, [
            'actor_type'   => $actorType,
            'actor_id'     => $actorId,
            'actor_name'   => $actorName,
            'actor_email'  => $actorEmail,
            'event_type'   => 'order.auto_confirmed_after_paid',
            'entity_type'  => 'order',
            'entity_id'    => $orderId,
            'entity_title' => $orderCode,
            'action'       => 'auto_confirm',
            'summary'      => 'Order moved to ' . $finalOrderStatus . ' and stock synced after paid path via ' . $source . ' | ' . bvoph_discount_summary_text($discount),
            'before_json'  => ['status' => $order['status'] ?? null],
            'after_json'   => [
                'status' => $finalOrderStatus,
                'payment_ref' => $paymentRef,
                'source' => $source,
                'discount' => $discount,
                'fee_snapshot' => $feeSnapshotResult,
            ],
        ]);

        $afterCommit = [
            'queue_notifications' => true,
            'notification_payload' => array_merge(
                bvoph_build_notification_payload($pdo, $orderId),
                ['source' => $source]
            ),
        ];

        if ($startedTx && $pdo->inTransaction()) {
            $pdo->commit();
        }

        bvoph_log('paid_handler_completed', [
            'order_id' => $orderId,
            'order_code' => $orderCode,
            'source' => $source,
            'stock_items' => count($stockChanges),
            'status' => $finalOrderStatus,
            'auction_items' => count(array_filter($stockChanges, static function ($r) {
                return (($r['mode'] ?? '') === 'auction');
            })),
            'discount' => $discount,
            'fee_snapshot' => $feeSnapshotResult,
        ]);

        if ($startedTx && !empty($afterCommit['queue_notifications'])) {
            bv_order_paid_handler_after_commit($pdo, $afterCommit);
        }

        return [
            'ok' => true,
            'order_id' => $orderId,
            'status' => $finalOrderStatus,
            'already_done' => false,
            'auto_confirmed' => in_array($finalOrderStatus, ['confirmed', 'processing', 'completed'], true),
            'errors' => [],
            'after_commit' => $afterCommit,
        ];
    } catch (Throwable $e) {
        if ($startedTx && $pdo->inTransaction()) {
            $pdo->rollBack();
        }

        bvoph_log('paid_handler_error', [
            'order_id' => $orderId,
            'source' => $source,
            'error' => $e->getMessage(),
        ]);

        return [
            'ok' => false,
            'order_id' => $orderId,
            'status' => null,
            'already_done' => false,
            'auto_confirmed' => false,
            'errors' => [$e->getMessage()],
            'after_commit' => [],
        ];
    }
}

function bv_run_order_paid_handler(PDO $pdo, int $orderId, array $context = []): array
{
    return bv_handle_paid_order($pdo, $orderId, $context);
}