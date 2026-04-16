<?php
declare(strict_types=1);

require_once __DIR__ . '/includes/db.php';
require_once __DIR__ . '/includes/order_paid_handler.php';



if (is_file(__DIR__ . '/includes/stripe_config.php')) {
    require_once __DIR__ . '/includes/stripe_config.php';
}

function bv_sw_debug(string $message, array $data = []): void
{
    $file = dirname(__DIR__) . '/private_html/stripe_debug.log';
    @file_put_contents(
        $file,
        '[' . date('Y-m-d H:i:s') . '] ' . $message . ' ' . json_encode($data, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES) . PHP_EOL,
        FILE_APPEND
    );
}

function bv_sw_log(string $event, array $data = []): void
{
    $file = dirname(__DIR__) . '/private_html/stripe_webhook.log';
    @file_put_contents(
        $file,
        '[' . date('Y-m-d H:i:s') . '] ' . $event . ' ' . json_encode($data, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES) . PHP_EOL,
        FILE_APPEND
    );
}

function bv_sw_json_response(int $code, array $data = []): void
{
    http_response_code($code);
    header('Content-Type: application/json; charset=utf-8');
    echo json_encode($data, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
    exit;
}

function bv_sw_get_header(string $name): string
{
    $key = 'HTTP_' . strtoupper(str_replace('-', '_', $name));
    return (string)($_SERVER[$key] ?? '');
}

function bv_sw_table_exists(PDO $pdo, string $table): bool
{
    static $cache = [];
    if (isset($cache[$table])) {
        return $cache[$table];
    }

    try {
        $safe = str_replace('`', '', $table);
        $stmt = $pdo->query("SHOW TABLES LIKE " . $pdo->quote($safe));
        return $cache[$table] = (bool)$stmt->fetchColumn();
    } catch (Throwable $e) {
        return false;
    }
}

function bv_sw_columns(PDO $pdo, string $table): array
{
    static $cache = [];
    if (isset($cache[$table])) {
        return $cache[$table];
    }

    $cols = [];
    try {
        $stmt = $pdo->query("SHOW COLUMNS FROM `{$table}`");
        while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
            $cols[$row['Field']] = true;
        }
    } catch (Throwable $e) {
        // ignore
    }

    return $cache[$table] = $cols;
}

function bv_sw_has_col(PDO $pdo, string $table, string $column): bool
{
    return isset(bv_sw_columns($pdo, $table)[$column]);
}

function bv_sw_verify_signature(string $payload, string $sigHeader, string $secret, int $tolerance = 3600): bool
{
    if ($payload === '' || $sigHeader === '' || $secret === '') {
        return false;
    }

    $parts = [];
    foreach (explode(',', $sigHeader) as $piece) {
        $piece = trim($piece);
        if (strpos($piece, '=') === false) {
            continue;
        }
        [$k, $v] = explode('=', $piece, 2);
        $parts[$k][] = $v;
    }

    $timestamp  = isset($parts['t'][0]) ? (int)$parts['t'][0] : 0;
    $signatures = $parts['v1'] ?? [];

    if ($timestamp <= 0 || !$signatures) {
        return false;
    }
    if (abs(time() - $timestamp) > $tolerance) {
        return false;
    }

    $expected = hash_hmac('sha256', $timestamp . '.' . $payload, $secret);
    foreach ($signatures as $sig) {
        if (hash_equals($expected, $sig)) {
            return true;
        }
    }

    return false;
}

function bv_sw_extract_order_id(array $event): int
{
    $obj  = $event['data']['object'] ?? [];
    $meta = (array)($obj['metadata'] ?? []);

    foreach (['order_id', 'bettavaro_order_id', 'bv_order_id'] as $key) {
        if (!empty($meta[$key]) && ctype_digit((string)$meta[$key])) {
            return (int)$meta[$key];
        }
    }

    if (!empty($obj['client_reference_id']) && ctype_digit((string)$obj['client_reference_id'])) {
        return (int)$obj['client_reference_id'];
    }

    return 0;
}

function bv_sw_find_order(PDO $pdo, int $orderId, bool $forUpdate = false): ?array
{
    $sql = 'SELECT * FROM orders WHERE id = ? LIMIT 1';
    if ($forUpdate) {
        $sql .= ' FOR UPDATE';
    }

    $stmt = $pdo->prepare($sql);
    $stmt->execute([$orderId]);
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    return $row ?: null;
}

function bv_sw_save_webhook_event(PDO $pdo, array $payload): void
{
    if (!bv_sw_table_exists($pdo, 'webhook_events')) {
        return;
    }

    $cols = bv_sw_columns($pdo, 'webhook_events');
    $data = [
        'provider'     => 'stripe',
        'event_id'     => $payload['event_id'] ?? null,
        'event_type'   => $payload['event_type'] ?? null,
        'object_id'    => $payload['object_id'] ?? null,
        'status'       => $payload['status'] ?? null,
        'payload_json' => isset($payload['payload_json']) ? json_encode($payload['payload_json'], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES) : null,
        'processed_at' => $payload['processed_at'] ?? date('Y-m-d H:i:s'),
        'created_at'   => $payload['created_at'] ?? date('Y-m-d H:i:s'),
    ];

    $insertCols = [];
    $insertVals = [];
    $params     = [];

    foreach ($data as $k => $v) {
        if (isset($cols[$k])) {
            $insertCols[] = "`{$k}`";
            $insertVals[] = '?';
            $params[]     = $v;
        }
    }

    if (!$insertCols) {
        return;
    }

    $stmt = $pdo->prepare(
        'INSERT INTO webhook_events (' . implode(', ', $insertCols) . ') VALUES (' . implode(', ', $insertVals) . ')'
    );
    $stmt->execute($params);
}

function bv_sw_has_processed_event(PDO $pdo, string $eventId): bool
{
    if ($eventId === '' || !bv_sw_table_exists($pdo, 'webhook_events')) {
        return false;
    }

    try {
        $stmt = $pdo->prepare('SELECT id FROM webhook_events WHERE event_id = ? LIMIT 1');
        $stmt->execute([$eventId]);
        return (bool)$stmt->fetchColumn();
    } catch (Throwable $e) {
        return false;
    }
}

function bv_sw_get_webhook_secret(): string
{
    if (isset($GLOBALS['BV_STRIPE_WEBHOOK_SECRET']) && (string)$GLOBALS['BV_STRIPE_WEBHOOK_SECRET'] !== '') {
        return (string)$GLOBALS['BV_STRIPE_WEBHOOK_SECRET'];
    }

    if (defined('BV_STRIPE_WEBHOOK_SECRET') && (string)BV_STRIPE_WEBHOOK_SECRET !== '') {
        return (string)BV_STRIPE_WEBHOOK_SECRET;
    }

    $mode = strtolower((string)($GLOBALS['BV_STRIPE_MODE'] ?? 'test'));
    $cfg  = $GLOBALS['BV_STRIPE_CONFIG'][$mode] ?? null;
    if (is_array($cfg) && !empty($cfg['webhook_secret'])) {
        return (string)$cfg['webhook_secret'];
    }

    return '';
}

bv_sw_debug('hit', [
    'method' => $_SERVER['REQUEST_METHOD'] ?? 'unknown',
    'uri'    => $_SERVER['REQUEST_URI'] ?? '',
]);

if (strtoupper((string)($_SERVER['REQUEST_METHOD'] ?? '')) !== 'POST') {
    bv_sw_log('method_not_allowed', [
        'method' => $_SERVER['REQUEST_METHOD'] ?? 'unknown',
    ]);
    bv_sw_json_response(405, ['ok' => false, 'error' => 'Method not allowed']);
}

if (!isset($pdo) || !($pdo instanceof PDO)) {
    bv_sw_log('bootstrap_error', ['message' => 'PDO not available']);
    bv_sw_json_response(500, ['ok' => false, 'error' => 'PDO not available']);
}

$webhookSecret = bv_sw_get_webhook_secret();
$payload       = file_get_contents('php://input') ?: '';
$sigHeader     = bv_sw_get_header('Stripe-Signature');

bv_sw_debug('before_verify', [
    'payload_len' => strlen($payload),
    'has_sig'     => $sigHeader !== '',
    'has_secret'  => $webhookSecret !== '',
    'mode'        => strtolower((string)($GLOBALS['BV_STRIPE_MODE'] ?? 'test')),
]);

if (!bv_sw_verify_signature($payload, $sigHeader, $webhookSecret)) {
    bv_sw_log('signature_invalid', [
        'has_payload' => $payload !== '',
        'has_sig'     => $sigHeader !== '',
        'has_secret'  => $webhookSecret !== '',
    ]);
    bv_sw_json_response(400, ['ok' => false, 'error' => 'Invalid signature']);
}

$event = json_decode($payload, true);
if (!is_array($event)) {
    bv_sw_log('invalid_json', []);
    bv_sw_json_response(400, ['ok' => false, 'error' => 'Invalid JSON']);
}

$eventId       = (string)($event['id'] ?? '');
$eventType     = (string)($event['type'] ?? '');
$orderId       = bv_sw_extract_order_id($event);
$eventLiveMode = !empty($event['livemode']);
$appMode       = strtolower((string)($GLOBALS['BV_STRIPE_MODE'] ?? 'test'));
$eventObject   = $event['data']['object'] ?? [];
$objectId      = '';

if (is_array($eventObject) && !empty($eventObject['id'])) {
    $objectId = (string)$eventObject['id'];
}

bv_sw_debug('event_decoded', [
    'event_id'    => $eventId,
    'event_type'  => $eventType,
    'order_id'    => $orderId,
    'object_id'   => $objectId,
    'event_live'  => $eventLiveMode,
    'app_mode'    => $appMode,
]);

if (($appMode === 'live' && !$eventLiveMode) || ($appMode !== 'live' && $eventLiveMode)) {
    try {
        if (!$pdo->inTransaction()) {
            $pdo->beginTransaction();
        }
        bv_sw_save_webhook_event($pdo, [
            'event_id'     => $eventId,
            'event_type'   => $eventType,
            'object_id'    => $objectId ?: null,
            'status'       => 'mode_mismatch',
            'payload_json' => $event,
            'processed_at' => date('Y-m-d H:i:s'),
        ]);
        if ($pdo->inTransaction()) {
            $pdo->commit();
        }
    } catch (Throwable $e) {
        if ($pdo->inTransaction()) {
            $pdo->rollBack();
        }
    }

    bv_sw_log('mode_mismatch_ignore', [
        'event_id' => $eventId,
        'event_type' => $eventType,
        'event_live' => $eventLiveMode,
        'app_mode' => $appMode,
    ]);

    bv_sw_json_response(200, ['ok' => true, 'ignored' => true, 'reason' => 'mode_mismatch']);
}

if ($eventId !== '' && bv_sw_has_processed_event($pdo, $eventId)) {
    bv_sw_log('duplicate_event_skip', ['event_id' => $eventId, 'event_type' => $eventType]);
    bv_sw_json_response(200, ['ok' => true, 'duplicate' => true]);
}

$processableTypes = [
    'checkout.session.completed',
    'checkout.session.async_payment_succeeded',
    'payment_intent.succeeded',
];

if (!in_array($eventType, $processableTypes, true)) {
    try {
        if (!$pdo->inTransaction()) {
            $pdo->beginTransaction();
        }
        bv_sw_save_webhook_event($pdo, [
            'event_id'     => $eventId,
            'event_type'   => $eventType,
            'object_id'    => $objectId ?: null,
            'status'       => 'ignored',
            'payload_json' => $event,
            'processed_at' => date('Y-m-d H:i:s'),
        ]);
        if ($pdo->inTransaction()) {
            $pdo->commit();
        }
    } catch (Throwable $e) {
        if ($pdo->inTransaction()) {
            $pdo->rollBack();
        }
        bv_sw_log('ignored_event_save_failed', ['event_id' => $eventId, 'error' => $e->getMessage()]);
    }

    bv_sw_json_response(200, ['ok' => true, 'ignored' => true, 'event_type' => $eventType]);
}

if ($orderId <= 0) {
    try {
        if (!$pdo->inTransaction()) {
            $pdo->beginTransaction();
        }
        bv_sw_save_webhook_event($pdo, [
            'event_id'     => $eventId,
            'event_type'   => $eventType,
            'object_id'    => $objectId ?: null,
            'status'       => 'no_order_id',
            'payload_json' => $event,
            'processed_at' => date('Y-m-d H:i:s'),
        ]);
        if ($pdo->inTransaction()) {
            $pdo->commit();
        }
    } catch (Throwable $e) {
        if ($pdo->inTransaction()) {
            $pdo->rollBack();
        }
    }

    bv_sw_log('order_id_missing', ['event_id' => $eventId, 'event_type' => $eventType]);
    bv_sw_json_response(200, ['ok' => true, 'warning' => 'order_id_missing']);
}

try {
    $pdo->beginTransaction();
    bv_sw_debug('tx_started', ['event_id' => $eventId, 'order_id' => $orderId]);

    $order = bv_sw_find_order($pdo, $orderId, true);
    if (!$order) {
        throw new RuntimeException('Order not found: ' . $orderId);
    }

    $obj = $event['data']['object'] ?? [];
    $currentStatus = (string)($order['status'] ?? '');

    $blockedStatuses = ['completed', 'cancelled', 'refunded'];
    if (in_array($currentStatus, $blockedStatuses, true)) {
        bv_sw_save_webhook_event($pdo, [
            'event_id'     => $eventId,
            'event_type'   => $eventType,
            'object_id'    => $objectId ?: null,
            'status'       => 'state_blocked',
            'payload_json' => $event,
            'processed_at' => date('Y-m-d H:i:s'),
        ]);

        $pdo->commit();

        bv_sw_log('state_blocked_skip', [
            'event_id' => $eventId,
            'event_type' => $eventType,
            'order_id' => $orderId,
            'current_status' => $currentStatus,
        ]);

        bv_sw_json_response(200, [
            'ok' => true,
            'ignored' => true,
            'reason' => 'state_blocked',
            'current_status' => $currentStatus,
        ]);
    }

    $orderPatch = [];
    if (!in_array($currentStatus, ['confirmed', 'processing', 'completed'], true)) {
        $orderPatch['status'] = 'paid';
    }

    if (bv_sw_has_col($pdo, 'orders', 'payment_status')) {
        $orderPatch['payment_status'] = 'paid';
    }
    if (bv_sw_has_col($pdo, 'orders', 'paid_at') && empty($order['paid_at'])) {
        $orderPatch['paid_at'] = date('Y-m-d H:i:s');
    }
    if (bv_sw_has_col($pdo, 'orders', 'stripe_session_id') && !empty($obj['id']) && strpos((string)$obj['id'], 'cs_') === 0) {
        $orderPatch['stripe_session_id'] = (string)$obj['id'];
    }
    if (bv_sw_has_col($pdo, 'orders', 'stripe_payment_intent_id') && !empty($obj['payment_intent'])) {
        $orderPatch['stripe_payment_intent_id'] = is_string($obj['payment_intent'])
            ? (string)$obj['payment_intent']
            : (string)($obj['payment_intent']['id'] ?? '');
    }
    if (bv_sw_has_col($pdo, 'orders', 'payment_ref')) {
        if (!empty($obj['payment_intent']) && is_string($obj['payment_intent'])) {
            $orderPatch['payment_ref'] = (string)$obj['payment_intent'];
        } elseif (!empty($obj['id'])) {
            $orderPatch['payment_ref'] = (string)$obj['id'];
        }
    }

    $sets   = [];
    $params = [];
    foreach ($orderPatch as $k => $v) {
        $sets[]   = "`{$k}` = ?";
        $params[] = $v;
    }

    if ($sets) {
        $params[] = $orderId;

        bv_sw_debug('before_order_update', ['order_patch' => $orderPatch]);
        $stmt = $pdo->prepare('UPDATE orders SET ' . implode(', ', $sets) . ' WHERE id = ?');
        $stmt->execute($params);
        bv_sw_debug('after_order_update', ['order_id' => $orderId]);
    } else {
        bv_sw_debug('skip_order_update', [
            'order_id' => $orderId,
            'current_status' => $currentStatus,
        ]);
    }

    bv_sw_debug('before_paid_handler', ['order_id' => $orderId]);
    $result = bv_handle_paid_order($pdo, $orderId, [
        'source'      => 'stripe_webhook',
        'actor_type'  => 'system',
        'actor_id'    => null,
        'actor_name'  => 'Stripe Webhook',
        'actor_email' => null,
    ]);
    bv_sw_debug('after_paid_handler', ['result' => $result]);

    bv_sw_save_webhook_event($pdo, [
        'event_id'     => $eventId,
        'event_type'   => $eventType,
        'object_id'    => $objectId ?: null,
        'status'       => !empty($result['ok']) ? 'processed' : 'failed',
        'payload_json' => $event,
        'processed_at' => date('Y-m-d H:i:s'),
    ]);

    if (empty($result['ok'])) {
        // business failure: save log, stop Stripe retry, return 200
        $pdo->commit();
        bv_sw_log('paid_handler_failed', [
            'event_id'   => $eventId,
            'event_type' => $eventType,
            'order_id'   => $orderId,
            'errors'     => (array)($result['errors'] ?? []),
        ]);
        bv_sw_json_response(200, [
            'ok'       => false,
            'handled'  => false,
            'order_id' => $orderId,
            'error'    => implode(' | ', (array)($result['errors'] ?? [])),
        ]);
    }

    $pdo->commit();
    bv_sw_debug('tx_committed', ['order_id' => $orderId]);

    $afterCommitResult = null;
    if (function_exists('bv_order_paid_handler_after_commit')) {
        bv_sw_debug('before_after_commit', ['order_id' => $orderId]);
        $afterCommitResult = bv_order_paid_handler_after_commit($pdo, $result['after_commit'] ?? []);
        bv_sw_debug('after_after_commit', ['result' => $afterCommitResult]);
    }

    bv_sw_log('webhook_processed', [
        'event_id'       => $eventId,
        'event_type'     => $eventType,
        'order_id'       => $orderId,
        'handler_status' => $result['status'] ?? null,
        'auto_confirmed' => $result['auto_confirmed'] ?? null,
        'already_done'   => $result['already_done'] ?? null,
        'after_commit'   => $afterCommitResult,
    ]);

    bv_sw_json_response(200, [
        'ok'             => true,
        'order_id'       => $orderId,
        'status'         => $result['status'] ?? 'paid',
        'auto_confirmed' => !empty($result['auto_confirmed']),
        'already_done'   => !empty($result['already_done']),
        'after_commit'   => $afterCommitResult,
    ]);
} catch (Throwable $e) {
    if ($pdo->inTransaction()) {
        $pdo->rollBack();
    }

    bv_sw_log('webhook_error', [
        'event_id'   => $eventId,
        'event_type' => $eventType,
        'order_id'   => $orderId,
        'error'      => $e->getMessage(),
        'trace'      => substr($e->getTraceAsString(), 0, 4000),
    ]);

    bv_sw_debug('fatal', [
        'event_id'   => $eventId,
        'event_type' => $eventType,
        'order_id'   => $orderId,
        'error'      => $e->getMessage(),
    ]);

    bv_sw_json_response(500, ['ok' => false, 'error' => $e->getMessage()]);
}
