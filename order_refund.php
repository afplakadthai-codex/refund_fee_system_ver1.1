<?php
declare(strict_types=1);

/*
 * Bettavaro Order Refund subsystem helpers.
 */

if (!function_exists('bv_order_refund_boot')) {
    function bv_order_refund_boot(): void
    {
        if (!isset($GLOBALS['bv_order_refund_booted'])) {
            $GLOBALS['bv_order_refund_booted'] = true;
            $GLOBALS['bv_order_refund_tx_level'] = 0;
            $GLOBALS['bv_order_refund_table_cache'] = [];

            $dbBootstrapCandidates = [
                dirname(__DIR__) . '/config/db.php',
                dirname(__DIR__) . '/includes/db.php',
                dirname(__DIR__) . '/db.php',
            ];
            foreach ($dbBootstrapCandidates as $candidate) {
                if (is_file($candidate)) {
                    require_once $candidate;
                    break;
                }
            }
        }
    }
}


if (!function_exists('bv_order_refund_db')) {
    function bv_order_refund_db()
    {
        bv_order_refund_boot();
        $keys = ['pdo', 'PDO', 'db', 'conn', 'mysqli'];
        foreach ($keys as $key) {
            if (!array_key_exists($key, $GLOBALS)) {
                continue;
            }
            $db = $GLOBALS[$key];
            if (bv_order_refund_is_pdo($db) || bv_order_refund_is_mysqli($db)) {
                return $db;
            }
        }

        throw new RuntimeException('Database connection is missing. Expected PDO or mysqli in $GLOBALS.');
    }
}

if (!function_exists('bv_order_refund_is_pdo')) {
    function bv_order_refund_is_pdo($db): bool
    {
        return $db instanceof PDO;
    }
}

if (!function_exists('bv_order_refund_is_mysqli')) {
    function bv_order_refund_is_mysqli($db): bool
    {
        return $db instanceof mysqli;
    }
}

if (!function_exists('bv_order_refund_mysqli_prepare_named')) {
    function bv_order_refund_mysqli_prepare_named(string $sql, array $params): array
    {
        if ($params === []) {
            return [$sql, []];
        }

        $ordered = [];
        $rebuilt = preg_replace_callback('/:[a-zA-Z_][a-zA-Z0-9_]*/', static function (array $m) use ($params, &$ordered): string {
            $name = substr($m[0], 1);
            if (!array_key_exists($name, $params)) {
                throw new RuntimeException('Missing SQL parameter: ' . $name);
            }
            $ordered[] = $params[$name];
            return '?';
        }, $sql);

        if ($rebuilt === null) {
            throw new RuntimeException('Failed to parse SQL parameters for mysqli.');
        }

        return [$rebuilt, $ordered];
    }
}

if (!function_exists('bv_order_refund_query_all')) {
    function bv_order_refund_query_all(string $sql, array $params = []): array
    {
        $db = bv_order_refund_db();

        if (bv_order_refund_is_pdo($db)) {
            $stmt = $db->prepare($sql);
            if (!$stmt) {
                throw new RuntimeException('PDO prepare failed.');
            }
            $stmt->execute($params);
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
            return is_array($rows) ? $rows : [];
        }

        [$msql, $ordered] = bv_order_refund_mysqli_prepare_named($sql, $params);
        $stmt = $db->prepare($msql);
        if (!$stmt) {
            throw new RuntimeException('mysqli prepare failed: ' . $db->error);
        }
        if ($ordered !== []) {
            $types = str_repeat('s', count($ordered));
            $stmt->bind_param($types, ...$ordered);
        }
        if (!$stmt->execute()) {
            $error = $stmt->error;
            $stmt->close();
            throw new RuntimeException('mysqli execute failed: ' . $error);
        }
        $result = $stmt->get_result();
        if (!$result) {
            $stmt->close();
            return [];
        }
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $stmt->close();
        return is_array($rows) ? $rows : [];
    }
}

if (!function_exists('bv_order_refund_query_one')) {
    function bv_order_refund_query_one(string $sql, array $params = []): ?array
    {
        $rows = bv_order_refund_query_all($sql, $params);
        return $rows[0] ?? null;
    }
}

if (!function_exists('bv_order_refund_execute')) {
    function bv_order_refund_execute(string $sql, array $params = []): array
    {
        $db = bv_order_refund_db();

        if (bv_order_refund_is_pdo($db)) {
            $stmt = $db->prepare($sql);
            if (!$stmt) {
                throw new RuntimeException('PDO prepare failed.');
            }
            $ok = $stmt->execute($params);
            if (!$ok) {
                throw new RuntimeException('PDO execute failed.');
            }
            return [
                'affected_rows' => $stmt->rowCount(),
                'last_insert_id' => (int)$db->lastInsertId(),
            ];
        }

        [$msql, $ordered] = bv_order_refund_mysqli_prepare_named($sql, $params);
        $stmt = $db->prepare($msql);
        if (!$stmt) {
            throw new RuntimeException('mysqli prepare failed: ' . $db->error);
        }
        if ($ordered !== []) {
            $types = str_repeat('s', count($ordered));
            $stmt->bind_param($types, ...$ordered);
        }
        if (!$stmt->execute()) {
            $error = $stmt->error;
            $stmt->close();
            throw new RuntimeException('mysqli execute failed: ' . $error);
        }
        $meta = [
            'affected_rows' => $stmt->affected_rows,
            'last_insert_id' => (int)$db->insert_id,
        ];
        $stmt->close();
        return $meta;
    }
}

if (!function_exists('bv_order_refund_begin_transaction')) {
    function bv_order_refund_begin_transaction(): void
    {
        bv_order_refund_boot();
        $db = bv_order_refund_db();
        if ((int)$GLOBALS['bv_order_refund_tx_level'] === 0) {
            if (bv_order_refund_is_pdo($db)) {
                $db->beginTransaction();
            } else {
                $db->begin_transaction();
            }
        }
        $GLOBALS['bv_order_refund_tx_level'] = (int)$GLOBALS['bv_order_refund_tx_level'] + 1;
    }
}

if (!function_exists('bv_order_refund_commit')) {
    function bv_order_refund_commit(): void
    {
        bv_order_refund_boot();
        $level = (int)$GLOBALS['bv_order_refund_tx_level'];
        if ($level <= 0) {
            return;
        }

        $level--;
        $GLOBALS['bv_order_refund_tx_level'] = $level;
        if ($level === 0) {
            $db = bv_order_refund_db();
            if (bv_order_refund_is_pdo($db)) {
                $db->commit();
            } else {
                $db->commit();
            }
        }
    }
}

if (!function_exists('bv_order_refund_rollback')) {
    function bv_order_refund_rollback(): void
    {
        bv_order_refund_boot();
        $level = (int)$GLOBALS['bv_order_refund_tx_level'];
        if ($level <= 0) {
            return;
        }
        $GLOBALS['bv_order_refund_tx_level'] = 0;
        $db = bv_order_refund_db();
        if (bv_order_refund_is_pdo($db)) {
            if ($db->inTransaction()) {
                $db->rollBack();
            }
        } else {
            $db->rollback();
        }
    }
}

if (!function_exists('bv_order_refund_require_session')) {
    function bv_order_refund_require_session(): void
    {
        if (session_status() !== PHP_SESSION_ACTIVE) {
            @session_start();
        }
    }
}

if (!function_exists('bv_order_refund_current_user_id')) {
    function bv_order_refund_current_user_id(): int
    {
        bv_order_refund_require_session();

        $nestedKeys = ['user', 'admin', 'member', 'auth_user'];
        foreach ($nestedKeys as $nk) {
            if (isset($_SESSION[$nk]) && is_array($_SESSION[$nk]) && isset($_SESSION[$nk]['id']) && is_numeric($_SESSION[$nk]['id'])) {
                return (int)$_SESSION[$nk]['id'];
            }
        }

        $keys = ['user_id', 'id', 'admin_id', 'member_id'];
        foreach ($keys as $k) {
            if (isset($_SESSION[$k]) && is_numeric($_SESSION[$k])) {
                return (int)$_SESSION[$k];
            }
        }
        return 0;
    }
}

if (!function_exists('bv_order_refund_current_user_role')) {
    function bv_order_refund_current_user_role(): string
    {
        bv_order_refund_require_session();

        $nestedRoles = [
            ['user', 'role'],
            ['admin', 'role'],
            ['auth_user', 'role'],
        ];
        foreach ($nestedRoles as $pair) {
            [$root, $key] = $pair;
            if (isset($_SESSION[$root]) && is_array($_SESSION[$root]) && isset($_SESSION[$root][$key]) && is_string($_SESSION[$root][$key]) && $_SESSION[$root][$key] !== '') {
                return strtolower(trim($_SESSION[$root][$key]));
            }
        }

        $keys = ['user_role', 'role', 'account_role', 'admin_role'];
        foreach ($keys as $k) {
            if (isset($_SESSION[$k]) && is_string($_SESSION[$k]) && $_SESSION[$k] !== '') {
                return strtolower(trim($_SESSION[$k]));
            }
        }
        return 'guest';
    }
}

if (!function_exists('bv_order_refund_is_admin_role')) {
    function bv_order_refund_is_admin_role(?string $role = null): bool
    {
        $role = strtolower(trim((string)($role ?? bv_order_refund_current_user_role())));
        return in_array($role, ['admin', 'superadmin', 'super_admin', 'owner', 'staff', 'support', 'manager'], true);
    }
}

if (!function_exists('bv_order_refund_now')) {
    function bv_order_refund_now(): string
    {
        return date('Y-m-d H:i:s');
    }
}


if (!function_exists('bv_order_refund_table_exists')) {
    function bv_order_refund_table_exists(string $tableName): bool
    {
        $tableName = trim($tableName);
        if ($tableName === '') {
            throw new InvalidArgumentException('Table name cannot be empty.');
        }

        bv_order_refund_boot();
        if (array_key_exists($tableName, $GLOBALS['bv_order_refund_table_cache'])) {
            return (bool)$GLOBALS['bv_order_refund_table_cache'][$tableName];
        }

        $row = bv_order_refund_query_one(
            'SELECT 1
             FROM information_schema.tables
             WHERE table_schema = DATABASE()
               AND table_name = :table_name
             LIMIT 1',
            ['table_name' => $tableName]
        );

        $exists = is_array($row) && $row !== [];
        $GLOBALS['bv_order_refund_table_cache'][$tableName] = $exists;
        return $exists;
    }
}


if (!function_exists('bv_order_refund_require_tables')) {
    function bv_order_refund_require_tables(): bool
    {
        $required = [
            'order_cancellations',
            'order_cancellation_items',
            'order_refunds',
            'order_refund_items',
            'order_refund_transactions',
            'order_financial_ledger',
        ];

        $missing = [];
        foreach ($required as $table) {
            if (!bv_order_refund_table_exists($table)) {
                $missing[] = $table;
            }
        }
        if ($missing !== []) {
            throw new RuntimeException('Missing required tables: ' . implode(', ', $missing));
        }

        return true;
    }
}

if (!function_exists('bv_order_refund_allowed_statuses')) {
    function bv_order_refund_allowed_statuses(): array
    {
        return ['draft', 'pending_approval', 'approved', 'processing', 'partially_refunded', 'refunded', 'rejected', 'failed', 'cancelled'];
    }
}

if (!function_exists('bv_order_refund_can_transition')) {
    function bv_order_refund_can_transition(string $from, string $to): bool
    {
        $from = strtolower(trim($from));
        $to = strtolower(trim($to));

        if ($from === $to) {
            return true;
        }

        $map = [
            'draft' => ['pending_approval', 'cancelled'],
            'pending_approval' => ['approved', 'rejected', 'cancelled'],
            'approved' => ['processing', 'cancelled', 'failed'],
            'processing' => ['partially_refunded', 'refunded', 'failed'],
            'partially_refunded' => ['processing', 'refunded', 'failed'],
            'refunded' => [],
            'rejected' => [],
            'failed' => ['processing', 'cancelled'],
            'cancelled' => [],
        ];

        return isset($map[$from]) && in_array($to, $map[$from], true);
    }
}

if (!function_exists('bv_order_refund_validate_amount')) {
    function bv_order_refund_validate_amount($amount): float
    {
        if (!is_numeric($amount)) {
            throw new InvalidArgumentException('Refund amount must be numeric.');
        }
        $value = round((float)$amount, 2);
        if ($value < 0) {
            throw new InvalidArgumentException('Refund amount cannot be negative.');
        }
        return $value;
    }
}

if (!function_exists('bv_order_refund_generate_code')) {
    function bv_order_refund_generate_code(): string
    {
        return 'RFN-' . date('YmdHis') . '-' . strtoupper(bin2hex(random_bytes(3)));
    }
}

if (!function_exists('bv_order_refund_get_by_id')) {
    function bv_order_refund_get_by_id(int $refundId): ?array
    {
        if ($refundId <= 0) {
            throw new InvalidArgumentException('Invalid refund ID.');
        }
        return bv_order_refund_query_one('SELECT * FROM order_refunds WHERE id = :id LIMIT 1', ['id' => $refundId]);
    }
}

if (!function_exists('bv_order_refund_get_by_code')) {
    function bv_order_refund_get_by_code(string $refundCode): ?array
    {
        $refundCode = trim($refundCode);
        if ($refundCode === '') {
            throw new InvalidArgumentException('Refund code is required.');
        }
        return bv_order_refund_query_one('SELECT * FROM order_refunds WHERE refund_code = :code LIMIT 1', ['code' => $refundCode]);
    }
}

if (!function_exists('bv_order_refund_get_by_cancellation_id')) {
    function bv_order_refund_get_by_cancellation_id(int $cancellationId): ?array
    {
        if ($cancellationId <= 0) {
            throw new InvalidArgumentException('Invalid cancellation ID.');
        }
        return bv_order_refund_query_one('SELECT * FROM order_refunds WHERE order_cancellation_id = :cid ORDER BY id DESC LIMIT 1', ['cid' => $cancellationId]);
    }
}

if (!function_exists('bv_order_refund_get_items')) {
    function bv_order_refund_get_items(int $refundId): array
    {
        if ($refundId <= 0) {
            throw new InvalidArgumentException('Invalid refund ID.');
        }
        return bv_order_refund_query_all('SELECT * FROM order_refund_items WHERE refund_id = :rid ORDER BY id ASC', ['rid' => $refundId]);
    }
}

if (!function_exists('bv_order_refund_get_transactions')) {
    function bv_order_refund_get_transactions(int $refundId): array
    {
        if ($refundId <= 0) {
            throw new InvalidArgumentException('Invalid refund ID.');
        }
        return bv_order_refund_query_all('SELECT * FROM order_refund_transactions WHERE refund_id = :rid ORDER BY id DESC', ['rid' => $refundId]);
    }
}

if (!function_exists('bv_order_refund_build_filter_where')) {
    function bv_order_refund_build_filter_where(array $filters): array
    {
        $where = [];
        $params = [];

        if (isset($filters['status']) && $filters['status'] !== '') {
            $where[] = 'r.status = :status';
            $params['status'] = (string)$filters['status'];
        }
        if (isset($filters['refund_mode']) && $filters['refund_mode'] !== '') {
            $where[] = 'r.refund_mode = :refund_mode';
            $params['refund_mode'] = (string)$filters['refund_mode'];
        }
        if (isset($filters['refund_source']) && $filters['refund_source'] !== '') {
            $where[] = 'r.refund_source = :refund_source';
            $params['refund_source'] = (string)$filters['refund_source'];
        }
        if (isset($filters['order_id']) && is_numeric($filters['order_id'])) {
            $where[] = 'r.order_id = :order_id';
            $params['order_id'] = (int)$filters['order_id'];
        }
        if (isset($filters['cancellation_id']) && is_numeric($filters['cancellation_id'])) {
            $where[] = 'r.order_cancellation_id = :cancellation_id';
            $params['cancellation_id'] = (int)$filters['cancellation_id'];
        }
        if (isset($filters['date_from']) && $filters['date_from'] !== '') {
            $where[] = 'r.created_at >= :date_from';
            $params['date_from'] = (string)$filters['date_from'];
        }
        if (isset($filters['date_to']) && $filters['date_to'] !== '') {
            $where[] = 'r.created_at <= :date_to';
            $params['date_to'] = (string)$filters['date_to'];
        }
        if (isset($filters['keyword']) && trim((string)$filters['keyword']) !== '') {
            $where[] = '(r.refund_code LIKE :kw OR r.refund_reason_text LIKE :kw OR r.admin_note LIKE :kw OR r.internal_note LIKE :kw)';
            $params['kw'] = '%' . trim((string)$filters['keyword']) . '%';
        }

        return [
            'sql' => $where === [] ? '1=1' : implode(' AND ', $where),
            'params' => $params,
        ];
    }
}

if (!function_exists('bv_order_refund_list')) {
    function bv_order_refund_list(array $filters = []): array
    {
        $f = bv_order_refund_build_filter_where($filters);
        $sql = 'SELECT r.* FROM order_refunds r WHERE ' . $f['sql'] . ' ORDER BY r.id DESC';
        return bv_order_refund_query_all($sql, $f['params']);
    }
}

if (!function_exists('bv_order_refund_summary')) {
    function bv_order_refund_summary(array $filters = []): array
    {
        $f = bv_order_refund_build_filter_where($filters);
        $sql = 'SELECT
                    COUNT(*) AS total_count,
                    SUM(CASE WHEN r.status = "pending_approval" THEN 1 ELSE 0 END) AS pending_approval_count,
                    SUM(CASE WHEN r.status = "approved" THEN 1 ELSE 0 END) AS approved_count,
                    SUM(CASE WHEN r.status = "processing" THEN 1 ELSE 0 END) AS processing_count,
                    SUM(CASE WHEN r.status = "partially_refunded" THEN 1 ELSE 0 END) AS partially_refunded_count,
                    SUM(CASE WHEN r.status = "refunded" THEN 1 ELSE 0 END) AS refunded_count,
                    SUM(CASE WHEN r.status = "failed" THEN 1 ELSE 0 END) AS failed_count,
                    SUM(CASE WHEN r.status = "rejected" THEN 1 ELSE 0 END) AS rejected_count,
                    SUM(CASE WHEN r.status = "cancelled" THEN 1 ELSE 0 END) AS cancelled_count,
                    COALESCE(SUM(r.requested_refund_amount), 0) AS requested_total,
                    COALESCE(SUM(r.approved_refund_amount), 0) AS approved_total,
                    COALESCE(SUM(r.actual_refunded_amount), 0) AS refunded_total
                FROM order_refunds r
                WHERE ' . $f['sql'];
        return bv_order_refund_query_one($sql, $f['params']) ?? [];
    }
}

if (!function_exists('bv_order_refund_terminal_statuses')) {
    function bv_order_refund_terminal_statuses(): array
    {
        return ['cancelled', 'canceled', 'refunded', 'completed', 'failed'];
    }
}

if (!function_exists('bv_order_refund_active_request_statuses')) {
    function bv_order_refund_active_request_statuses(): array
    {
        return ['draft', 'pending_approval', 'approved', 'processing', 'partially_refunded'];
    }
}

if (!function_exists('bv_order_refund_normalize_order_status')) {
    function bv_order_refund_normalize_order_status($status): string
    {
        return strtolower(trim((string)$status));
    }
}

if (!function_exists('bv_order_refund_normalize_payment_status')) {
    function bv_order_refund_normalize_payment_status($status): string
    {
        return strtolower(trim((string)$status));
    }
}

if (!function_exists('bv_order_refund_is_terminal_status')) {
    function bv_order_refund_is_terminal_status(string $status): bool
    {
        return in_array(bv_order_refund_normalize_order_status($status), bv_order_refund_terminal_statuses(), true);
    }
}

if (!function_exists('bv_order_refund_is_paid_payment_status')) {
    function bv_order_refund_is_paid_payment_status(string $paymentStatus): bool
    {
        $paymentStatus = bv_order_refund_normalize_payment_status($paymentStatus);
        return in_array($paymentStatus, ['paid', 'succeeded', 'success', 'completed', 'captured'], true);
    }
}

if (!function_exists('bv_order_refund_is_unpaid_payment_status')) {
    function bv_order_refund_is_unpaid_payment_status(string $paymentStatus): bool
    {
        $paymentStatus = bv_order_refund_normalize_payment_status($paymentStatus);
        return in_array($paymentStatus, ['unpaid', 'pending', 'failed', 'void', 'refunded'], true);
    }
}

if (!function_exists('bv_order_refund_get_order_by_id')) {
    function bv_order_refund_get_order_by_id(int $orderId): ?array
    {
        if ($orderId <= 0) {
            return null;
        }

        return bv_order_refund_query_one('SELECT * FROM orders WHERE id = :id LIMIT 1', ['id' => $orderId]);
    }
}

if (!function_exists('bv_order_refund_find_latest_by_order_id')) {
    function bv_order_refund_find_latest_by_order_id(int $orderId): ?array
    {
        if ($orderId <= 0) {
            return null;
        }

        return bv_order_refund_query_one('SELECT * FROM order_refunds WHERE order_id = :order_id ORDER BY id DESC LIMIT 1', ['order_id' => $orderId]);
    }
}

if (!function_exists('bv_order_refund_find_open_by_order_id')) {
    function bv_order_refund_find_open_by_order_id(int $orderId): ?array
    {
        if ($orderId <= 0) {
            return null;
        }

        $statuses = bv_order_refund_active_request_statuses();
        $placeholders = [];
        $params = ['order_id' => $orderId];
        foreach ($statuses as $i => $status) {
            $key = 's' . $i;
            $placeholders[] = ':' . $key;
            $params[$key] = $status;
        }

        $sql = 'SELECT * FROM order_refunds WHERE order_id = :order_id AND status IN (' . implode(',', $placeholders) . ') ORDER BY id DESC LIMIT 1';
        return bv_order_refund_query_one($sql, $params);
    }
}

if (!function_exists('bv_order_refund_has_successful_refund_for_order')) {
    function bv_order_refund_has_successful_refund_for_order(int $orderId): bool
    {
        if ($orderId <= 0) {
            return false;
        }

        $row = bv_order_refund_query_one(
            'SELECT id FROM order_refunds
             WHERE order_id = :order_id
               AND (status = :status_refunded OR status = :status_partial OR actual_refunded_amount > 0)
             ORDER BY id DESC
             LIMIT 1',
            [
                'order_id' => $orderId,
                'status_refunded' => 'refunded',
                'status_partial' => 'partially_refunded',
            ]
        );

        return $row !== null;
    }
}

if (!function_exists('bv_order_refund_find_cancellation_by_order_id')) {
    function bv_order_refund_find_cancellation_by_order_id(int $orderId): ?array
    {
        if ($orderId <= 0) {
            return null;
        }

        return bv_order_refund_query_one(
            'SELECT * FROM order_cancellations
             WHERE order_id = :order_id
             ORDER BY id DESC
             LIMIT 1',
            ['order_id' => $orderId]
        );
    }
}

if (!function_exists('bv_order_refund_map_source')) {
    function bv_order_refund_map_source(string $source): string
    {
        $source = strtolower(trim($source));
        if ($source === 'buyer' || $source === 'buyer_request') {
            return 'buyer_request';
        }
        if ($source === 'seller') {
            return 'seller';
        }
        if ($source === 'admin') {
            return 'admin';
        }
        if ($source === 'system') {
            return 'system';
        }
        return 'buyer_request';
    }
}

if (!function_exists('bv_order_refund_is_allowed')) {
    function bv_order_refund_is_allowed(array $order, int $actorUserId = 0, string $actorRole = 'buyer_request'): bool
    {
        try {
            if ($order === []) {
                return false;
            }

            $orderId = (int)($order['id'] ?? 0);
            if ($orderId <= 0) {
                return false;
            }

            $orderStatus = bv_order_refund_normalize_order_status($order['status'] ?? '');
            $paymentStatus = bv_order_refund_normalize_payment_status($order['payment_status'] ?? $order['payment_state'] ?? '');
            $paymentState = bv_order_refund_normalize_payment_status($order['payment_state'] ?? '');

            if (bv_order_refund_is_terminal_status($orderStatus)) {
                return false;
            }

            if (bv_order_refund_is_unpaid_payment_status($paymentStatus) || bv_order_refund_is_unpaid_payment_status($paymentState)) {
                return false;
            }

            $isPaid = bv_order_refund_is_paid_payment_status($paymentStatus) || bv_order_refund_is_paid_payment_status($paymentState);
            if (!$isPaid) {
                return false;
            }

            if (!in_array($orderStatus, ['paid', 'confirmed', 'processing'], true)) {
                return false;
            }

            $role = strtolower(trim($actorRole));
            if ($role === '') {
                $role = 'buyer_request';
            }

            $isPrivilegedRole = in_array($role, ['admin', 'superadmin', 'super_admin', 'seller', 'system', 'staff', 'support', 'manager'], true);

            if (!$isPrivilegedRole && $actorUserId > 0) {
                $buyerId = 0;
                $buyerKeys = ['buyer_user_id', 'user_id', 'member_id'];
                foreach ($buyerKeys as $key) {
                    if (isset($order[$key]) && is_numeric($order[$key])) {
                        $buyerId = (int)$order[$key];
                        break;
                    }
                }

                if ($buyerId > 0 && $buyerId !== $actorUserId) {
                    return false;
                }
            }

            $open = bv_order_refund_find_open_by_order_id($orderId);
            if ($open) {
                return false;
            }

            if (bv_order_refund_has_successful_refund_for_order($orderId)) {
                return false;
            }

            return true;
        } catch (Throwable $e) {
            return false;
        }
    }
}

if (!function_exists('bv_order_refund_create_request')) {
    function bv_order_refund_create_request(array $data): array
    {
        bv_order_refund_require_tables();

        $orderId = isset($data['order_id']) ? (int)$data['order_id'] : 0;
        if ($orderId <= 0) {
            throw new RuntimeException('order_id is required.');
        }

        $order = bv_order_refund_get_order_by_id($orderId);
        if (!$order) {
            throw new RuntimeException('Order not found.');
        }

        $actorUserId = 0;
        if (isset($data['actor_user_id']) && is_numeric($data['actor_user_id'])) {
            $actorUserId = (int)$data['actor_user_id'];
        } elseif (isset($data['requested_by_user_id']) && is_numeric($data['requested_by_user_id'])) {
            $actorUserId = (int)$data['requested_by_user_id'];
        } else {
            $actorUserId = bv_order_refund_current_user_id();
        }

        $actorRole = '';
        if (isset($data['actor_role']) && is_string($data['actor_role']) && trim($data['actor_role']) !== '') {
            $actorRole = strtolower(trim((string)$data['actor_role']));
        } elseif (isset($data['requested_by_role']) && is_string($data['requested_by_role']) && trim($data['requested_by_role']) !== '') {
            $actorRole = strtolower(trim((string)$data['requested_by_role']));
        } elseif (isset($data['refund_source']) && is_string($data['refund_source']) && trim($data['refund_source']) !== '') {
            $actorRole = strtolower(trim((string)$data['refund_source']));
        } else {
            $actorRole = bv_order_refund_current_user_role();
        }

        $isPrivilegedActorRole = in_array($actorRole, ['admin', 'superadmin', 'super_admin', 'seller', 'system', 'staff', 'support', 'manager'], true);
        if (!$isPrivilegedActorRole && $actorUserId <= 0) {
            throw new RuntimeException('A logged-in actor is required for refund request.');
        }

        if (!bv_order_refund_is_allowed($order, $actorUserId, $actorRole)) {
            throw new RuntimeException('Refund request is not allowed for this order.');
        }

        $existingOpen = bv_order_refund_find_open_by_order_id($orderId);
        if ($existingOpen) {
            throw new RuntimeException('An active refund request already exists for this order.');
        }

        $cancellationId = 0;
        if (isset($data['order_cancellation_id']) && is_numeric($data['order_cancellation_id'])) {
            $cancellationId = (int)$data['order_cancellation_id'];
        }
        if ($cancellationId <= 0 && isset($data['cancellation_id']) && is_numeric($data['cancellation_id'])) {
            $cancellationId = (int)$data['cancellation_id'];
        }

        $cancellation = null;
        if ($cancellationId > 0) {
            $cancellation = bv_order_refund_query_one('SELECT * FROM order_cancellations WHERE id = :id LIMIT 1', ['id' => $cancellationId]);
            if ($cancellation && (int)($cancellation['order_id'] ?? 0) !== $orderId) {
                $cancellation = null;
                $cancellationId = 0;
            }
        }

        if ($cancellationId <= 0) {
            $cancellation = bv_order_refund_find_cancellation_by_order_id($orderId);
            $cancellationId = (int)($cancellation['id'] ?? 0);
        }

        if ($cancellationId <= 0 || !$cancellation) {
            throw new RuntimeException('Refund request requires cancellation bridge.');
        }

        $requestedAmount = 0.0;
        if (isset($data['requested_refund_amount']) && is_numeric($data['requested_refund_amount'])) {
            $requestedAmount = bv_order_refund_validate_amount($data['requested_refund_amount']);
        }

        if ($requestedAmount <= 0) {
            $requestedAmount = bv_order_refund_validate_amount(
                $cancellation['approved_refund_amount']
                ?? $cancellation['refundable_amount']
                ?? $cancellation['total_snapshot']
                ?? $order['total']
                ?? 0
            );
        }

        $safeMax = bv_order_refund_validate_amount($cancellation['refundable_amount'] ?? ($cancellation['total_snapshot'] ?? ($order['total'] ?? $requestedAmount)));
        if ($safeMax > 0 && $requestedAmount > $safeMax) {
            $requestedAmount = $safeMax;
        }
        $requestedAmount = round(max(0.0, $requestedAmount), 2);

        $requestSource = bv_order_refund_map_source((string)($data['refund_source'] ?? $actorRole));
        $requestedByRole = bv_order_refund_map_source((string)($data['requested_by_role'] ?? $actorRole));

        $payload = [
            'order_id' => $orderId,
            'cancellation_id' => $cancellationId,
            'requested_refund_amount' => $requestedAmount,
            'refund_reason_code' => (string)($data['refund_reason_code'] ?? ($cancellation['cancel_reason_code'] ?? '')),
            'refund_reason_text' => (string)($data['refund_reason_text'] ?? ($cancellation['cancel_reason_text'] ?? '')),
            'refund_source' => $requestSource,
            'requested_by_user_id' => isset($data['requested_by_user_id']) && is_numeric($data['requested_by_user_id'])
                ? (int)$data['requested_by_user_id']
                : $actorUserId,
            'requested_by_role' => $requestedByRole,
            'actor_user_id' => $actorUserId,
            'actor_role' => $requestedByRole,
            'admin_note' => (string)($data['admin_note'] ?? ''),
            'internal_note' => (string)($data['internal_note'] ?? ''),
        ];

        return bv_order_refund_create_from_cancellation($payload);
    }
}

if (!function_exists('bv_order_refund_create_from_cancellation')) {
    function bv_order_refund_create_from_cancellation(array $data): array
    {
        bv_order_refund_require_tables();

        $cancellationId = isset($data['cancellation_id']) ? (int)$data['cancellation_id'] : 0;
        if ($cancellationId <= 0) {
            throw new InvalidArgumentException('cancellation_id is required.');
        }

        $allowDuplicate = !empty($data['allow_duplicate']);
        $actorUserId = isset($data['actor_user_id']) ? (int)$data['actor_user_id'] : bv_order_refund_current_user_id();
        $actorRole = isset($data['actor_role']) && is_string($data['actor_role']) ? strtolower(trim($data['actor_role'])) : bv_order_refund_current_user_role();
        $now = bv_order_refund_now();

        bv_order_refund_begin_transaction();
        try {
            $cancellation = bv_order_refund_query_one(
                'SELECT * FROM order_cancellations WHERE id = :id LIMIT 1 FOR UPDATE',
                ['id' => $cancellationId]
            );
            if (!$cancellation) {
                throw new RuntimeException('Cancellation not found for refund creation.');
            }

            $existing = bv_order_refund_get_by_cancellation_id($cancellationId);
            if ($existing && !$allowDuplicate) {
                $existingStatus = strtolower(trim((string)($existing['status'] ?? '')));
                if (in_array($existingStatus, ['rejected', 'cancelled', 'failed'], true)) {
                    // Allow retry after terminal non-successful refund states.
                } elseif (in_array($existingStatus, ['draft', 'pending_approval', 'approved', 'processing'], true)) {
                    throw new RuntimeException('An active refund request already exists for this cancellation.');
                } elseif (in_array($existingStatus, ['refunded', 'partially_refunded'], true)) {
                    throw new RuntimeException('A refund already exists for this cancellation.');
                } else {
                    throw new RuntimeException('A refund already exists for this cancellation.');
                }
            }

            $cancelItems = bv_order_refund_query_all(
                'SELECT * FROM order_cancellation_items WHERE cancellation_id = :cid ORDER BY id ASC FOR UPDATE',
                ['cid' => $cancellationId]
            );

            $refundCode = bv_order_refund_generate_code();
            $requested = isset($data['requested_refund_amount'])
                ? bv_order_refund_validate_amount($data['requested_refund_amount'])
                : bv_order_refund_validate_amount($cancellation['approved_refund_amount'] ?? ($cancellation['refundable_amount'] ?? 0));
            $safeMax = bv_order_refund_validate_amount($cancellation['refundable_amount'] ?? $requested);
            if ($requested > $safeMax) {
                throw new RuntimeException('Requested refund amount exceeds cancellation refundable amount.');
            }

            $itemsTotalMax = 0.0;
            foreach ($cancelItems as $ci) {
                $itemsTotalMax += bv_order_refund_validate_amount($ci['item_refundable_amount'] ?? ($ci['refund_line_amount'] ?? 0));
            }
            $scopeTotal = max($safeMax, $itemsTotalMax);
            $refundMode = ($scopeTotal > 0 && $requested >= $scopeTotal) ? 'full' : 'partial';

            $insert = bv_order_refund_execute(
                'INSERT INTO order_refunds (
                    order_id, order_cancellation_id, refund_code,
                    refund_source, refund_reason_code, refund_reason_text,
                    status, refund_mode, currency,
                    subtotal_snapshot, discount_snapshot, shipping_snapshot, tax_snapshot, order_total_snapshot,
                    already_refunded_amount_snapshot, requested_refund_amount, approved_refund_amount, actual_refunded_amount,
                    payment_provider, payment_reference_snapshot, payment_status_snapshot,
                    order_status_snapshot, order_source_snapshot, restock_state_snapshot,
                    requested_by_user_id, requested_by_role, requested_at,
                    admin_note, internal_note, created_at, updated_at
                ) VALUES (
                    :order_id, :order_cancellation_id, :refund_code,
                    :refund_source, :refund_reason_code, :refund_reason_text,
                    :status, :refund_mode, :currency,
                    :subtotal_snapshot, :discount_snapshot, :shipping_snapshot, :tax_snapshot, :order_total_snapshot,
                    :already_refunded_amount_snapshot, :requested_refund_amount, :approved_refund_amount, :actual_refunded_amount,
                    :payment_provider, :payment_reference_snapshot, :payment_status_snapshot,
                    :order_status_snapshot, :order_source_snapshot, :restock_state_snapshot,
                    :requested_by_user_id, :requested_by_role, :requested_at,
                    :admin_note, :internal_note, :created_at, :updated_at
                )',
                [
                    'order_id' => (int)$cancellation['order_id'],
                    'order_cancellation_id' => $cancellationId,
                    'refund_code' => $refundCode,
                    'refund_source' => (static function (string $source): string {
                        $source = strtolower(trim($source));
                        if ($source === 'buyer' || $source === 'buyer_request') {
                            return 'buyer_request';
                        }
                        if ($source === 'seller') {
                            return 'seller';
                        }
                        if ($source === 'admin') {
                            return 'admin';
                        }
                        if ($source === 'system') {
                            return 'system';
                        }
                        return 'system';
                    })((string)($data['refund_source'] ?? ($cancellation['cancel_source'] ?? 'system'))),
                    'refund_reason_code' => (string)($data['refund_reason_code'] ?? ($cancellation['cancel_reason_code'] ?? '')),
                    'refund_reason_text' => (string)($data['refund_reason_text'] ?? ($cancellation['cancel_reason_text'] ?? '')),
                    'status' => 'pending_approval',
                    'refund_mode' => $refundMode,
                    'currency' => (string)($cancellation['currency'] ?? 'USD'),
                    'subtotal_snapshot' => bv_order_refund_validate_amount($cancellation['subtotal_before_discount_snapshot'] ?? 0),
                    'discount_snapshot' => bv_order_refund_validate_amount($cancellation['discount_amount_snapshot'] ?? 0),
                    'shipping_snapshot' => bv_order_refund_validate_amount($cancellation['shipping_amount_snapshot'] ?? 0),
                    'tax_snapshot' => bv_order_refund_validate_amount($data['tax_snapshot'] ?? 0),
                    'order_total_snapshot' => bv_order_refund_validate_amount($cancellation['total_snapshot'] ?? 0),
                    'already_refunded_amount_snapshot' => bv_order_refund_validate_amount($data['already_refunded_amount_snapshot'] ?? 0),
                    'requested_refund_amount' => $requested,
                    'approved_refund_amount' => 0,
                    'actual_refunded_amount' => 0,
                    'payment_provider' => (string)($data['payment_provider'] ?? ''),
                    'payment_reference_snapshot' => (string)($data['payment_reference_snapshot'] ?? ($cancellation['refund_reference'] ?? '')),
                    'payment_status_snapshot' => (string)($cancellation['payment_state_snapshot'] ?? ''),
                    'order_status_snapshot' => (string)($cancellation['order_status_snapshot'] ?? ''),
                    'order_source_snapshot' => (string)($cancellation['order_source_snapshot'] ?? ''),
                    'restock_state_snapshot' => (string)($data['restock_state_snapshot'] ?? ''),
                    'requested_by_user_id' => $actorUserId > 0 ? $actorUserId : (int)($cancellation['requested_by_user_id'] ?? 0),
                    'requested_by_role' => $actorRole !== '' ? $actorRole : (string)($cancellation['requested_by_role'] ?? 'system'),
                    'requested_at' => (string)($data['requested_at'] ?? $now),
                    'admin_note' => (string)($data['admin_note'] ?? ($cancellation['admin_note'] ?? '')),
                    'internal_note' => (string)($data['internal_note'] ?? ''),
                    'created_at' => $now,
                    'updated_at' => $now,
                ]
            );

            $refundId = (int)$insert['last_insert_id'];
            if ($refundId <= 0) {
                throw new RuntimeException('Failed to create refund header.');
            }

            $createdItems = [];
            $remainingToDistribute = $requested;
            foreach ($cancelItems as $ci) {
                $max = bv_order_refund_validate_amount($ci['item_refundable_amount'] ?? ($ci['refund_line_amount'] ?? 0));
                $reqLine = 0.0;
                if ($remainingToDistribute > 0 && $max > 0) {
                    $reqLine = $remainingToDistribute >= $max ? $max : $remainingToDistribute;
                    $remainingToDistribute = round($remainingToDistribute - $reqLine, 2);
                    if ($remainingToDistribute < 0) {
                        $remainingToDistribute = 0.0;
                    }
                }

                $itemInsert = bv_order_refund_execute(
                    'INSERT INTO order_refund_items (
                        refund_id, order_cancellation_item_id, order_item_id, listing_id,
                        qty_snapshot, unit_price_snapshot, line_total_snapshot,
                        max_refundable_amount, requested_refund_amount, approved_refund_amount, actual_refunded_amount,
                        refund_type, note, created_at, updated_at
                    ) VALUES (
                        :refund_id, :order_cancellation_item_id, :order_item_id, :listing_id,
                        :qty_snapshot, :unit_price_snapshot, :line_total_snapshot,
                        :max_refundable_amount, :requested_refund_amount, :approved_refund_amount, :actual_refunded_amount,
                        :refund_type, :note, :created_at, :updated_at
                    )',
                    [
                        'refund_id' => $refundId,
                        'order_cancellation_item_id' => (int)$ci['id'],
                        'order_item_id' => (int)($ci['order_item_id'] ?? 0),
                        'listing_id' => (int)($ci['listing_id'] ?? 0),
                        'qty_snapshot' => (int)($ci['refund_qty'] ?? $ci['qty'] ?? 0),
                        'unit_price_snapshot' => bv_order_refund_validate_amount($ci['unit_price_snapshot'] ?? 0),
                        'line_total_snapshot' => bv_order_refund_validate_amount($ci['line_total_snapshot'] ?? 0),
                        'max_refundable_amount' => $max,
                        'requested_refund_amount' => $reqLine,
                        'approved_refund_amount' => 0,
                        'actual_refunded_amount' => 0,
                        'refund_type' => 'item',
                        'note' => '',
                        'created_at' => $now,
                        'updated_at' => $now,
                    ]
                );
                $createdItems[] = bv_order_refund_query_one('SELECT * FROM order_refund_items WHERE id = :id LIMIT 1', ['id' => (int)$itemInsert['last_insert_id']]);
            }

            $requestedSum = 0.0;
            foreach ($createdItems as $createdItem) {
                if (!is_array($createdItem)) {
                    continue;
                }
                $requestedSum += bv_order_refund_validate_amount($createdItem['requested_refund_amount'] ?? 0);
            }
            $requestedSum = round($requestedSum, 2);
            if (abs($requestedSum - $requested) > 0.01) {
                throw new RuntimeException('Refund item requested amount sum does not match refund header requested amount.');
            }

            bv_order_refund_execute(
                'UPDATE order_cancellations
                 SET refund_id = :rid, refund_status = :refund_status, updated_at = :updated_at
                 WHERE id = :cid',
                [
                    'rid' => $refundId,
                    'refund_status' => 'pending',
                    'updated_at' => $now,
                    'cid' => $cancellationId,
                ]
            );

            bv_order_refund_insert_ledger([
                'order_id' => (int)$cancellation['order_id'],
                'refund_id' => $refundId,
                'event_type' => 'refund_created',
                'amount' => $requested,
                'currency' => (string)($cancellation['currency'] ?? 'USD'),
                'actor_user_id' => $actorUserId,
                'actor_role' => $actorRole,
                'note' => 'Refund created from cancellation snapshot',
                'created_at' => $now,
            ]);

            bv_order_refund_commit();

            return [
                'refund' => bv_order_refund_get_by_id($refundId),
                'items' => bv_order_refund_get_items($refundId),
            ];
        } catch (Throwable $e) {
            bv_order_refund_rollback();
            throw $e;
        }
    }
}

if (!function_exists('bv_order_refund_approve')) {
    function bv_order_refund_approve(int $refundId, float $approvedAmount, ?int $actorUserId = null, ?string $actorRole = null, string $note = ''): array
    {
        $approvedAmount = bv_order_refund_validate_amount($approvedAmount);
        $actorUserId = $actorUserId ?? bv_order_refund_current_user_id();
        $actorRole = strtolower(trim((string)($actorRole ?? bv_order_refund_current_user_role())));
        $now = bv_order_refund_now();

        bv_order_refund_begin_transaction();
        try {
            $refund = bv_order_refund_query_one('SELECT * FROM order_refunds WHERE id = :id LIMIT 1 FOR UPDATE', ['id' => $refundId]);
            if (!$refund) {
                throw new RuntimeException('Refund not found.');
            }
            if (!bv_order_refund_can_transition((string)$refund['status'], 'approved')) {
                throw new RuntimeException('Invalid refund status transition to approved.');
            }
            $max = bv_order_refund_validate_amount($refund['requested_refund_amount'] ?? 0);
            if ($approvedAmount > $max) {
                throw new RuntimeException('Approved amount cannot exceed requested amount.');
            }
            $mode = ($max > 0 && $approvedAmount >= $max) ? 'full' : 'partial';

            bv_order_refund_execute(
                'UPDATE order_refunds
                 SET status = :status,
                     refund_mode = :refund_mode,
                     approved_refund_amount = :approved_refund_amount,
                     approved_by_user_id = :approved_by_user_id,
                     approved_at = :approved_at,
                     admin_note = :admin_note,
                     updated_at = :updated_at
                 WHERE id = :id',
                [
                    'status' => 'approved',
                    'refund_mode' => $mode,
                    'approved_refund_amount' => $approvedAmount,
                    'approved_by_user_id' => $actorUserId,
                    'approved_at' => $now,
                    'admin_note' => $note,
                    'updated_at' => $now,
                    'id' => $refundId,
                ]
            );

            $items = bv_order_refund_query_all(
                'SELECT * FROM order_refund_items WHERE refund_id = :refund_id ORDER BY id ASC FOR UPDATE',
                ['refund_id' => $refundId]
            );
            $remaining = $approvedAmount;
            $allocated = 0.0;
            foreach ($items as $item) {
                $requestedLine = bv_order_refund_validate_amount($item['requested_refund_amount'] ?? 0);
                $lineApproved = 0.0;
                if ($remaining > 0 && $requestedLine > 0) {
                    $lineApproved = $remaining >= $requestedLine ? $requestedLine : $remaining;
                }
                $lineApproved = round($lineApproved, 2);
                $remaining = round($remaining - $lineApproved, 2);
                if ($remaining < 0) {
                    $remaining = 0.0;
                }
                $allocated += $lineApproved;

                bv_order_refund_execute(
                    'UPDATE order_refund_items SET approved_refund_amount = :approved_refund_amount, updated_at = :updated_at WHERE id = :id',
                    [
                        'approved_refund_amount' => $lineApproved,
                        'updated_at' => $now,
                        'id' => (int)$item['id'],
                    ]
                );
            }

            $sumRow = bv_order_refund_query_one(
                'SELECT COALESCE(SUM(approved_refund_amount), 0) AS sum_approved FROM order_refund_items WHERE refund_id = :refund_id',
                ['refund_id' => $refundId]
            );
            $sumApproved = bv_order_refund_validate_amount($sumRow['sum_approved'] ?? 0);
            if (abs($sumApproved - $approvedAmount) > 0.01) {
                throw new RuntimeException('Approved refund allocation mismatch between header and items.');
            }

            bv_order_refund_insert_ledger([
                'order_id' => (int)$refund['order_id'],
                'refund_id' => $refundId,
                'event_type' => 'refund_approved',
                'amount' => $approvedAmount,
                'currency' => (string)($refund['currency'] ?? 'USD'),
                'actor_user_id' => (int)$actorUserId,
                'actor_role' => $actorRole,
                'note' => $note,
                'created_at' => $now,
            ]);

            bv_order_refund_sync_cancellation_bridge($refundId);
            bv_order_refund_commit();
            return bv_order_refund_get_by_id($refundId) ?? [];
        } catch (Throwable $e) {
            bv_order_refund_rollback();
            throw $e;
        }
    }
}


if (!function_exists('bv_order_refund_mark_processing')) {
    function bv_order_refund_mark_processing(int $refundId, ?int $actorUserId = null, string $note = '', ?string $actorRole = null): array
    {
        $actorUserId = $actorUserId ?? bv_order_refund_current_user_id();
        $actorRole = strtolower(trim((string)($actorRole ?? bv_order_refund_current_user_role())));
        if ($actorRole === '') {
            $actorRole = 'system';
        }
        $now = bv_order_refund_now();

        bv_order_refund_begin_transaction();
        try {
            $refund = bv_order_refund_query_one('SELECT * FROM order_refunds WHERE id = :id LIMIT 1 FOR UPDATE', ['id' => $refundId]);
            if (!$refund) {
                throw new RuntimeException('Refund not found.');
            }
            if (!bv_order_refund_can_transition((string)$refund['status'], 'processing')) {
                throw new RuntimeException('Invalid refund status transition to processing.');
            }

            bv_order_refund_execute(
                'UPDATE order_refunds
                 SET status = :status,
                     processed_by_user_id = :processed_by_user_id,
                     processing_started_at = :processing_started_at,
                     internal_note = :internal_note,
                     updated_at = :updated_at
                 WHERE id = :id',
                [
                    'status' => 'processing',
                    'processed_by_user_id' => $actorUserId,
                    'processing_started_at' => $now,
                    'internal_note' => $note,
                    'updated_at' => $now,
                    'id' => $refundId,
                ]
            );

            bv_order_refund_insert_ledger([
                'order_id' => (int)$refund['order_id'],
                'refund_id' => $refundId,
                'event_type' => 'refund_processing',
                'amount' => bv_order_refund_validate_amount($refund['approved_refund_amount'] ?? 0),
                'currency' => (string)($refund['currency'] ?? 'USD'),
                'actor_user_id' => (int)$actorUserId,
                'actor_role' => $actorRole,
                'note' => $note,
                'created_at' => $now,
            ]);

            bv_order_refund_sync_cancellation_bridge($refundId);
            bv_order_refund_commit();
            return bv_order_refund_get_by_id($refundId) ?? [];
        } catch (Throwable $e) {
            bv_order_refund_rollback();
            throw $e;
        }
    }
}


if (!function_exists('bv_order_refund_mark_refunded')) {
    function bv_order_refund_mark_refunded(int $refundId, float $actualAmount, array $transactionData = [], ?int $actorUserId = null, string $note = '', ?string $actorRole = null): array
    {
        $actualAmount = bv_order_refund_validate_amount($actualAmount);
        $actorUserId = $actorUserId ?? bv_order_refund_current_user_id();
        $actorRole = strtolower(trim((string)($actorRole ?? bv_order_refund_current_user_role())));
        if ($actorRole === '') {
            $actorRole = 'system';
        }
        $now = bv_order_refund_now();

        bv_order_refund_begin_transaction();
        try {
            $refund = bv_order_refund_query_one('SELECT * FROM order_refunds WHERE id = :id LIMIT 1 FOR UPDATE', ['id' => $refundId]);
            if (!$refund) {
                throw new RuntimeException('Refund not found.');
            }

            $approved = bv_order_refund_validate_amount($refund['approved_refund_amount'] ?? 0);
            if ($approved <= 0) {
                throw new RuntimeException('Refund must be approved with positive amount before marking refunded.');
            }

            $currentActual = bv_order_refund_validate_amount($refund['actual_refunded_amount'] ?? 0);
            $cumulativeActual = round($currentActual + $actualAmount, 2);
            if ($cumulativeActual > $approved) {
                throw new RuntimeException('Cumulative refunded amount exceeds approved amount.');
            }

            if (!bv_order_refund_can_transition((string)$refund['status'], 'refunded')
                && !bv_order_refund_can_transition((string)$refund['status'], 'partially_refunded')) {
                throw new RuntimeException('Invalid refund status transition for refund completion.');
            }

            $toStatus = ($cumulativeActual < $approved) ? 'partially_refunded' : 'refunded';

            bv_order_refund_execute(
                'UPDATE order_refunds
                 SET status = :status,
                     actual_refunded_amount = :actual_refunded_amount,
                     processed_by_user_id = :processed_by_user_id,
                     refunded_at = :refunded_at,
                     internal_note = :internal_note,
                     updated_at = :updated_at
                 WHERE id = :id',
                [
                    'status' => $toStatus,
                    'actual_refunded_amount' => $cumulativeActual,
                    'processed_by_user_id' => $actorUserId,
                    'refunded_at' => $now,
                    'internal_note' => $note,
                    'updated_at' => $now,
                    'id' => $refundId,
                ]
            );

            $items = bv_order_refund_query_all(
                'SELECT * FROM order_refund_items WHERE refund_id = :refund_id ORDER BY id ASC FOR UPDATE',
                ['refund_id' => $refundId]
            );
            $remainingTarget = $cumulativeActual;
            foreach ($items as $item) {
                $approvedLine = bv_order_refund_validate_amount($item['approved_refund_amount'] ?? 0);
                $lineActual = 0.0;
                if ($remainingTarget > 0 && $approvedLine > 0) {
                    $lineActual = $remainingTarget >= $approvedLine ? $approvedLine : $remainingTarget;
                }
                $lineActual = round($lineActual, 2);
                $remainingTarget = round($remainingTarget - $lineActual, 2);
                if ($remainingTarget < 0) {
                    $remainingTarget = 0.0;
                }

                bv_order_refund_execute(
                    'UPDATE order_refund_items SET actual_refunded_amount = :actual_refunded_amount, updated_at = :updated_at WHERE id = :id',
                    [
                        'actual_refunded_amount' => $lineActual,
                        'updated_at' => $now,
                        'id' => (int)$item['id'],
                    ]
                );
            }

            $sumRow = bv_order_refund_query_one(
                'SELECT COALESCE(SUM(actual_refunded_amount), 0) AS sum_actual FROM order_refund_items WHERE refund_id = :refund_id',
                ['refund_id' => $refundId]
            );
            $sumActual = bv_order_refund_validate_amount($sumRow['sum_actual'] ?? 0);
            if (abs($sumActual - $cumulativeActual) > 0.01) {
                throw new RuntimeException('Actual refunded allocation mismatch between header and items.');
            }

            if ($transactionData !== []) {
                $transactionData['refund_id'] = $refundId;
                $transactionData['amount'] = $transactionData['amount'] ?? $actualAmount;
                $transactionData['currency'] = $transactionData['currency'] ?? (string)($refund['currency'] ?? 'USD');
                $transactionData['status'] = $transactionData['status'] ?? 'succeeded';
                bv_order_refund_insert_transaction($transactionData);
            }

            $eventType = $toStatus === 'partially_refunded' ? 'refund_partially_refunded' : 'refund_refunded';
            bv_order_refund_insert_ledger([
                'order_id' => (int)$refund['order_id'],
                'refund_id' => $refundId,
                'event_type' => $eventType,
                'amount' => $actualAmount,
                'currency' => (string)($refund['currency'] ?? 'USD'),
                'actor_user_id' => (int)$actorUserId,
                'actor_role' => $actorRole,
                'note' => $note,
                'created_at' => $now,
            ]);

            bv_order_refund_sync_cancellation_bridge($refundId);
            bv_order_refund_commit();
            return bv_order_refund_get_by_id($refundId) ?? [];
        } catch (Throwable $e) {
            bv_order_refund_rollback();
            throw $e;
        }
    }
}


if (!function_exists('bv_order_refund_mark_failed')) {
    function bv_order_refund_mark_failed(int $refundId, string $reason = '', array $transactionData = [], ?int $actorUserId = null, ?string $actorRole = null): array
    {
        $actorUserId = $actorUserId ?? bv_order_refund_current_user_id();
        $actorRole = strtolower(trim((string)($actorRole ?? bv_order_refund_current_user_role())));
        if ($actorRole === '') {
            $actorRole = 'system';
        }
        $now = bv_order_refund_now();

        bv_order_refund_begin_transaction();
        try {
            $refund = bv_order_refund_query_one('SELECT * FROM order_refunds WHERE id = :id LIMIT 1 FOR UPDATE', ['id' => $refundId]);
            if (!$refund) {
                throw new RuntimeException('Refund not found.');
            }
            if (!bv_order_refund_can_transition((string)$refund['status'], 'failed')) {
                throw new RuntimeException('Invalid refund status transition to failed.');
            }

            bv_order_refund_execute(
                'UPDATE order_refunds
                 SET status = :status,
                     failed_by_user_id = :failed_by_user_id,
                     failed_at = :failed_at,
                     internal_note = :internal_note,
                     updated_at = :updated_at
                 WHERE id = :id',
                [
                    'status' => 'failed',
                    'failed_by_user_id' => $actorUserId,
                    'failed_at' => $now,
                    'internal_note' => $reason,
                    'updated_at' => $now,
                    'id' => $refundId,
                ]
            );

            if ($transactionData !== []) {
                $transactionData['refund_id'] = $refundId;
                $transactionData['status'] = $transactionData['status'] ?? 'failed';
                $transactionData['error_message'] = $transactionData['error_message'] ?? $reason;
                $transactionData['currency'] = $transactionData['currency'] ?? (string)($refund['currency'] ?? 'USD');
                bv_order_refund_insert_transaction($transactionData);
            }

            bv_order_refund_insert_ledger([
                'order_id' => (int)$refund['order_id'],
                'refund_id' => $refundId,
                'event_type' => 'refund_failed',
                'amount' => bv_order_refund_validate_amount($refund['approved_refund_amount'] ?? 0),
                'currency' => (string)($refund['currency'] ?? 'USD'),
                'actor_user_id' => (int)$actorUserId,
                'actor_role' => $actorRole,
                'note' => $reason,
                'created_at' => $now,
            ]);

            bv_order_refund_sync_cancellation_bridge($refundId);
            bv_order_refund_commit();
            return bv_order_refund_get_by_id($refundId) ?? [];
        } catch (Throwable $e) {
            bv_order_refund_rollback();
            throw $e;
        }
    }
}


if (!function_exists('bv_order_refund_reject')) {
    function bv_order_refund_reject(int $refundId, string $reason = '', ?int $actorUserId = null, ?string $actorRole = null): array
    {
        $actorUserId = $actorUserId ?? bv_order_refund_current_user_id();
        $actorRole = strtolower(trim((string)($actorRole ?? bv_order_refund_current_user_role())));
        $now = bv_order_refund_now();

        bv_order_refund_begin_transaction();
        try {
            $refund = bv_order_refund_query_one('SELECT * FROM order_refunds WHERE id = :id LIMIT 1 FOR UPDATE', ['id' => $refundId]);
            if (!$refund) {
                throw new RuntimeException('Refund not found.');
            }
            if (!bv_order_refund_can_transition((string)$refund['status'], 'rejected')) {
                throw new RuntimeException('Invalid refund status transition to rejected.');
            }

            bv_order_refund_execute(
                'UPDATE order_refunds
                 SET status = :status,
                     rejected_by_user_id = :rejected_by_user_id,
                     rejected_at = :rejected_at,
                     admin_note = :admin_note,
                     updated_at = :updated_at
                 WHERE id = :id',
                [
                    'status' => 'rejected',
                    'rejected_by_user_id' => $actorUserId,
                    'rejected_at' => $now,
                    'admin_note' => $reason,
                    'updated_at' => $now,
                    'id' => $refundId,
                ]
            );

            bv_order_refund_insert_ledger([
                'order_id' => (int)$refund['order_id'],
                'refund_id' => $refundId,
                'event_type' => 'refund_rejected',
                'amount' => bv_order_refund_validate_amount($refund['requested_refund_amount'] ?? 0),
                'currency' => (string)($refund['currency'] ?? 'USD'),
                'actor_user_id' => (int)$actorUserId,
                'actor_role' => $actorRole,
                'note' => $reason,
                'created_at' => $now,
            ]);

            bv_order_refund_sync_cancellation_bridge($refundId);
            bv_order_refund_commit();
            return bv_order_refund_get_by_id($refundId) ?? [];
        } catch (Throwable $e) {
            bv_order_refund_rollback();
            throw $e;
        }
    }
}

if (!function_exists('bv_order_refund_cancel')) {
    function bv_order_refund_cancel(int $refundId, string $reason = '', ?int $actorUserId = null, ?string $actorRole = null): array
    {
        $actorUserId = $actorUserId ?? bv_order_refund_current_user_id();
        $actorRole = strtolower(trim((string)($actorRole ?? bv_order_refund_current_user_role())));
        $now = bv_order_refund_now();

        bv_order_refund_begin_transaction();
        try {
            $refund = bv_order_refund_query_one('SELECT * FROM order_refunds WHERE id = :id LIMIT 1 FOR UPDATE', ['id' => $refundId]);
            if (!$refund) {
                throw new RuntimeException('Refund not found.');
            }
            if (!bv_order_refund_can_transition((string)$refund['status'], 'cancelled')) {
                throw new RuntimeException('Invalid refund status transition to cancelled.');
            }

            bv_order_refund_execute(
                'UPDATE order_refunds
                 SET status = :status,
                     admin_note = :admin_note,
                     updated_at = :updated_at
                 WHERE id = :id',
                [
                    'status' => 'cancelled',
                    'admin_note' => $reason,
                    'updated_at' => $now,
                    'id' => $refundId,
                ]
            );

            bv_order_refund_insert_ledger([
                'order_id' => (int)$refund['order_id'],
                'refund_id' => $refundId,
                'event_type' => 'refund_cancelled',
                'amount' => bv_order_refund_validate_amount($refund['requested_refund_amount'] ?? 0),
                'currency' => (string)($refund['currency'] ?? 'USD'),
                'actor_user_id' => (int)$actorUserId,
                'actor_role' => $actorRole,
                'note' => $reason,
                'created_at' => $now,
            ]);

            bv_order_refund_sync_cancellation_bridge($refundId);
            bv_order_refund_commit();
            return bv_order_refund_get_by_id($refundId) ?? [];
        } catch (Throwable $e) {
            bv_order_refund_rollback();
            throw $e;
        }
    }
}

if (!function_exists('bv_order_refund_insert_transaction')) {
    function bv_order_refund_insert_transaction(array $data): int
    {
        if (!bv_order_refund_table_exists('order_refund_transactions')) {
            return 0;
        }

        $refundId = isset($data['refund_id']) ? (int)$data['refund_id'] : 0;
        if ($refundId <= 0) {
            throw new InvalidArgumentException('refund_id is required for transaction insert.');
        }

        $status = strtolower(trim((string)($data['transaction_status'] ?? ($data['status'] ?? 'pending'))));
        if (!in_array($status, ['pending', 'succeeded', 'failed', 'cancelled'], true)) {
            throw new InvalidArgumentException('Invalid transaction_status. Allowed: pending, succeeded, failed, cancelled.');
        }

        $transactionType = trim((string)($data['transaction_type'] ?? ''));
        if ($transactionType === '') {
            if ($status === 'succeeded') {
                $transactionType = 'provider_refund';
            } elseif ($status === 'failed') {
                $transactionType = 'failure';
            } else {
                $transactionType = 'request';
            }
        }

        $requestPayload = $data['raw_request_payload'] ?? ($data['raw_payload'] ?? ($data['payload_json'] ?? null));
        $responsePayload = $data['raw_response_payload'] ?? ($data['payload_json'] ?? ($data['raw_payload'] ?? null));
        if (is_array($requestPayload) || is_object($requestPayload)) {
            $requestPayload = json_encode($requestPayload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        }
        if (is_array($responsePayload) || is_object($responsePayload)) {
            $responsePayload = json_encode($responsePayload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        }

        $result = bv_order_refund_execute(
            'INSERT INTO order_refund_transactions
             (refund_id, transaction_type, transaction_status, provider, provider_refund_id, provider_payment_intent_id,
              currency, amount, raw_request_payload, raw_response_payload, failure_code, failure_message, created_by_user_id, created_at)
             VALUES
             (:refund_id, :transaction_type, :transaction_status, :provider, :provider_refund_id, :provider_payment_intent_id,
              :currency, :amount, :raw_request_payload, :raw_response_payload, :failure_code, :failure_message, :created_by_user_id, :created_at)',
            [
                'refund_id' => $refundId,
                'transaction_type' => $transactionType,
                'transaction_status' => $status,
                'provider' => (string)($data['provider'] ?? ''),
                'provider_refund_id' => (string)($data['provider_refund_id'] ?? ($data['provider_reference'] ?? '')),
                'provider_payment_intent_id' => (string)($data['provider_payment_intent_id'] ?? ''),
                'currency' => (string)($data['currency'] ?? 'USD'),
                'amount' => bv_order_refund_validate_amount($data['amount'] ?? 0),
                'raw_request_payload' => (string)($requestPayload ?? ''),
                'raw_response_payload' => (string)($responsePayload ?? ''),
                'failure_code' => (string)($data['failure_code'] ?? ''),
                'failure_message' => (string)($data['failure_message'] ?? ($data['error_message'] ?? '')),
                'created_by_user_id' => (int)($data['created_by_user_id'] ?? bv_order_refund_current_user_id()),
                'created_at' => (string)($data['created_at'] ?? bv_order_refund_now()),
            ]
        );

        return (int)$result['last_insert_id'];
    }
}


if (!function_exists('bv_order_refund_insert_ledger')) {
    function bv_order_refund_insert_ledger(array $data): int
    {
        if (!bv_order_refund_table_exists('order_financial_ledger')) {
            return 0;
        }

        $eventType = trim((string)($data['event_type'] ?? ''));
        $entryType = trim((string)($data['entry_type'] ?? ''));
        $direction = trim((string)($data['direction'] ?? ''));

        if ($entryType === '' || $direction === '') {
            $eventMap = [
                'refund_created' => ['entry_type' => 'refund_adjustment', 'direction' => 'out'],
                'refund_approved' => ['entry_type' => 'refund_adjustment', 'direction' => 'out'],
                'refund_processing' => ['entry_type' => 'refund_adjustment', 'direction' => 'out'],
                'refund_partially_refunded' => ['entry_type' => 'refund_out', 'direction' => 'out'],
                'refund_refunded' => ['entry_type' => 'refund_out', 'direction' => 'out'],
                'refund_failed' => ['entry_type' => 'refund_adjustment', 'direction' => 'out'],
                'refund_rejected' => ['entry_type' => 'refund_adjustment', 'direction' => 'out'],
                'refund_cancelled' => ['entry_type' => 'refund_adjustment', 'direction' => 'out'],
            ];
            if ($eventType !== '' && isset($eventMap[$eventType])) {
                $entryType = $entryType !== '' ? $entryType : $eventMap[$eventType]['entry_type'];
                $direction = $direction !== '' ? $direction : $eventMap[$eventType]['direction'];
            }
        }

        if ($entryType === '') {
            $entryType = 'refund_adjustment';
        }
        if ($direction === '') {
            $direction = 'out';
        }

        $refundId = (int)($data['refund_id'] ?? 0);
        $referenceType = (string)($data['reference_type'] ?? 'refund');
        $referenceId = isset($data['reference_id']) ? (string)$data['reference_id'] : ($refundId > 0 ? (string)$refundId : '');

        $result = bv_order_refund_execute(
            'INSERT INTO order_financial_ledger
             (order_id, refund_id, entry_type, direction, currency, amount,
              reference_type, reference_id, provider, provider_reference,
              memo, entry_status, created_by_user_id, created_at)
             VALUES
             (:order_id, :refund_id, :entry_type, :direction, :currency, :amount,
              :reference_type, :reference_id, :provider, :provider_reference,
              :memo, :entry_status, :created_by_user_id, :created_at)',
            [
                'order_id' => (int)($data['order_id'] ?? 0),
                'refund_id' => $refundId,
                'entry_type' => $entryType,
                'direction' => $direction,
                'currency' => (string)($data['currency'] ?? 'USD'),
                'amount' => bv_order_refund_validate_amount($data['amount'] ?? 0),
                'reference_type' => $referenceType,
                'reference_id' => $referenceId,
                'provider' => (string)($data['provider'] ?? ''),
                'provider_reference' => (string)($data['provider_reference'] ?? ''),
                'memo' => (string)($data['memo'] ?? ($data['note'] ?? '')),
                'entry_status' => (string)($data['entry_status'] ?? 'posted'),
                'created_by_user_id' => (int)($data['created_by_user_id'] ?? ($data['actor_user_id'] ?? 0)),
                'created_at' => (string)($data['created_at'] ?? bv_order_refund_now()),
            ]
        );

        return (int)$result['last_insert_id'];
    }
}


if (!function_exists('bv_order_refund_sync_cancellation_bridge')) {
    function bv_order_refund_sync_cancellation_bridge(int $refundId): void
    {
        $refund = bv_order_refund_get_by_id($refundId);
        if (!$refund) {
            throw new RuntimeException('Refund not found for cancellation bridge sync.');
        }

        $cancellationId = (int)($refund['order_cancellation_id'] ?? 0);
        if ($cancellationId <= 0) {
            return;
        }

        $cancellation = bv_order_refund_query_one('SELECT * FROM order_cancellations WHERE id = :id LIMIT 1 FOR UPDATE', ['id' => $cancellationId]);
        if (!$cancellation) {
            return;
        }

        $tx = bv_order_refund_query_one(
            'SELECT provider_refund_id, provider_payment_intent_id
             FROM order_refund_transactions
             WHERE refund_id = :rid
               AND (provider_refund_id <> "" OR provider_payment_intent_id <> "")
             ORDER BY id DESC
             LIMIT 1',
            ['rid' => $refundId]
        );

        $reference = (string)($cancellation['refund_reference'] ?? '');
        if (!empty($tx['provider_refund_id'])) {
            $reference = (string)$tx['provider_refund_id'];
        } elseif (!empty($tx['provider_payment_intent_id'])) {
            $reference = (string)$tx['provider_payment_intent_id'];
        }

        $fromStatus = strtolower((string)($refund['status'] ?? ''));
        $bridgeStatus = (string)($cancellation['refund_status'] ?? '');
        if ($fromStatus === 'pending_approval') {
            $bridgeStatus = $bridgeStatus !== '' ? $bridgeStatus : 'pending';
        } elseif ($fromStatus === 'approved') {
            $bridgeStatus = 'ready';
        } elseif ($fromStatus === 'processing') {
            $bridgeStatus = 'processing';
        } elseif ($fromStatus === 'partially_refunded') {
            $bridgeStatus = 'partially_refunded';
        } elseif ($fromStatus === 'refunded') {
            $bridgeStatus = 'refunded';
        } elseif ($fromStatus === 'failed') {
            $bridgeStatus = 'failed';
        } elseif (in_array($fromStatus, ['rejected', 'cancelled'], true)) {
            if (!in_array($bridgeStatus, ['refunded', 'partially_refunded', 'processing', 'failed'], true)) {
                $bridgeStatus = $bridgeStatus !== '' ? $bridgeStatus : 'pending';
            }
        }

        bv_order_refund_execute(
            'UPDATE order_cancellations
             SET refund_id = :refund_id,
                 approved_refund_amount = :approved_refund_amount,
                 refund_reference = :refund_reference,
                 refund_status = :refund_status,
                 updated_at = :updated_at
             WHERE id = :id',
            [
                'refund_id' => $refundId,
                'approved_refund_amount' => bv_order_refund_validate_amount($refund['approved_refund_amount'] ?? 0),
                'refund_reference' => $reference,
                'refund_status' => $bridgeStatus,
                'updated_at' => bv_order_refund_now(),
                'id' => $cancellationId,
            ]
        );
    }
}


bv_order_refund_boot();
