package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/roshaans/schema"
	"github.com/streamingfast/logging"
	orderedmap "github.com/wk8/go-ordered-map/v2"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Make the typing a bit easier
type OrderedMap[K comparable, V any] struct {
	*orderedmap.OrderedMap[K, V]
}

func NewOrderedMap[K comparable, V any]() *OrderedMap[K, V] {
	return &OrderedMap[K, V]{OrderedMap: orderedmap.New[K, V]()}
}

type SystemTableError struct {
	error
}

type Loader struct {
	*sql.DB

	entries      *OrderedMap[string, *OrderedMap[string, *Operation]]
	entriesCount uint64
	tables       map[string]*TableInfo
	cursorTable  *TableInfo

	handleReorgs            bool
	batchBlockFlushInterval int
	batchRowFlushInterval   int
	liveBlockFlushInterval  int
	moduleMismatchMode      OnModuleHashMismatch

	dialect Dialect

	logger *zap.Logger
	tracer logging.Tracer

	testTx *TestTx // used for testing: if non-nil, 'loader.BeginTx()' will return this object instead of a real *sql.Tx
	dsn    *DSN
}

func NewLoader(
	dsn *DSN,
	cursorTableName string, historyTableName string, clickhouseCluster string,
	batchBlockFlushInterval int,
	batchRowFlushInterval int,
	liveBlockFlushInterval int,
	OnModuleHashMismatch string,
	handleReorgs *bool,
	logger *zap.Logger,
	tracer logging.Tracer,
) (*Loader, error) {

	sqlDB, err := sql.Open(dsn.Driver(), dsn.ConnString())
	if err != nil {
		return nil, fmt.Errorf("open db connection: %w", err)
	}

	dialect, err := newDialect(sqlDB.Driver(), dsn.Schema(), cursorTableName, historyTableName, clickhouseCluster)
	if err != nil {
		return nil, fmt.Errorf("get dialect: %w", err)
	}

	moduleMismatchMode, err := ParseOnModuleHashMismatch(OnModuleHashMismatch)
	if err != nil {
		return nil, fmt.Errorf("parse on module hash mismatch: %w", err)
	}

	l := &Loader{
		DB:                      sqlDB,
		dsn:                     dsn,
		entries:                 NewOrderedMap[string, *OrderedMap[string, *Operation]](),
		tables:                  map[string]*TableInfo{},
		batchBlockFlushInterval: batchBlockFlushInterval,
		batchRowFlushInterval:   batchRowFlushInterval,
		liveBlockFlushInterval:  liveBlockFlushInterval,
		moduleMismatchMode:      moduleMismatchMode,
		dialect:                 dialect,
		logger:                  logger,
		tracer:                  tracer,
	}

	if handleReorgs == nil {
		// automatic detection
		l.handleReorgs = !l.dialect.OnlyInserts()
	} else {
		l.handleReorgs = *handleReorgs
	}

	if l.handleReorgs && l.dialect.OnlyInserts() {
		return nil, fmt.Errorf("driver %s does not support reorg handling. You must use set a non-zero undo-buffer-size", sqlDB.Driver())
	}

	logger.Info("created new DB loader",
		zap.Int("batch_block_flush_interval", batchBlockFlushInterval),
		zap.Int("batch_row_flush_interval", batchRowFlushInterval),
		zap.Int("live_block_flush_interval", liveBlockFlushInterval),
		zap.Stringer("on_module_hash_mismatch", moduleMismatchMode),
		zap.Bool("handle_reorgs", l.handleReorgs),
		zap.String("dialect", fmt.Sprintf("%T", l.dialect)),
	)

	return l, nil
}

func newDialect(driver driver.Driver, schemaName string, cursorTableName string, historyTableName string, clickHouseClusterName string) (Dialect, error) {
	driverType := fmt.Sprintf("%T", driver)
	switch driverType {
	case "*pq.Driver":
		return NewPostgresDialect(schemaName, cursorTableName, historyTableName), nil
	case "*clickhouse.stdDriver":
		return NewClickhouseDialect(schemaName, cursorTableName, clickHouseClusterName), nil
	default:
		return nil, fmt.Errorf("unsupported driver: %s", driverType)
	}
}

type Tx interface {
	Rollback() error
	Commit() error
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func (l *Loader) Begin() (Tx, error) {
	return l.BeginTx(context.Background(), nil)
}

func (l *Loader) BeginTx(ctx context.Context, opts *sql.TxOptions) (Tx, error) {
	if l.testTx != nil {
		return l.testTx, nil
	}
	return l.DB.BeginTx(ctx, opts)
}

func (l *Loader) BatchBlockFlushInterval() int {
	return l.batchBlockFlushInterval
}

func (l *Loader) LiveBlockFlushInterval() int {
	return l.liveBlockFlushInterval
}

func (l *Loader) FlushNeeded() bool {
	totalRows := 0
	// todo keep a running count when inserting/deleting rows directly
	for pair := l.entries.Oldest(); pair != nil; pair = pair.Next() {
		totalRows += pair.Value.Len()
	}
	return totalRows > l.batchRowFlushInterval
}

func (l *Loader) LoadTables(schemaName string, cursorTableName string, historyTableName string) error {
	l.logger.Info("starting LoadTables",
		zap.String("schema_name", schemaName),
		zap.String("cursor_table", cursorTableName),
		zap.String("history_table", historyTableName),
	)

	query := `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = ?
	`
	rows, err := l.DB.Query(query, schemaName)
	if err != nil {
		l.logger.Error("failed to query tables", zap.Error(err))
		return fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	schemaTables := make(map[[2]string][]*sql.ColumnType)
	var foundTables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return fmt.Errorf("failed to scan table name: %w", err)
		}
		foundTables = append(foundTables, tableName)

		if tableName != "dex_trades" && tableName != cursorTableName {
			l.logger.Debug("skipping column type fetch for table", zap.String("table", tableName))
			continue
		}

		query := fmt.Sprintf("SELECT * FROM %s.%s LIMIT 1", schemaName, tableName)
		l.logger.Debug("attempting column type fetch",
			zap.String("table", tableName),
			zap.String("query", query),
			zap.String("schema", schemaName),
			zap.String("tableName", tableName),
			zap.Bool("is_cursor_table", tableName == cursorTableName),
			zap.Bool("is_history_table", tableName == historyTableName),
		)
		cols, err := l.DB.Query(query)
		if err != nil {
			l.logger.Warn("skipping table due to query error",
				zap.String("table", tableName),
				zap.String("query", query),
				zap.Error(err),
			)
			continue
		}
		l.logger.Debug("query executed successfully", zap.String("table", tableName))

		var columnTypes []*sql.ColumnType
		func() {
			defer func() {
				if r := recover(); r != nil {
					l.logger.Warn("panic while fetching column types", zap.String("table", tableName), zap.Any("recover", r))
					columnTypes = nil
				}
			}()
			columnTypes, err = cols.ColumnTypes()
		}()
		cols.Close()
		if err != nil {
			l.logger.Warn("skipping table due to column type error", zap.String("table", tableName), zap.Error(err))
			continue
		}
		if columnTypes == nil {
			l.logger.Warn("skipping table due to nil column types", zap.String("table", tableName))
			continue
		}
		schemaTables[[2]string{schemaName, tableName}] = columnTypes
	}

	l.logger.Debug("retrieved schema tables",
		zap.Int("table_count", len(schemaTables)),
		zap.Strings("table_names", foundTables),
	)

	seenCursorTable := false
	seenHistoryTable := false
	for schemaTableName, columns := range schemaTables {
		tableName := schemaTableName[1]
		l.logger.Debug("processing schemaName's table",
			zap.String("schema_name", schemaName),
			zap.String("table_name", tableName),
		)

		if tableName == cursorTableName {
			l.logger.Info("matched cursor table name", zap.String("table_name", tableName))
			l.logger.Debug("validating cursor table", zap.String("table_name", tableName), zap.Int("column_count", len(columns)))
			for _, col := range columns {
				l.logger.Debug("cursor table column",
					zap.String("name", col.Name()),
					zap.String("db_type", col.DatabaseTypeName()),
					zap.String("scan_type", fmt.Sprintf("%v", col.ScanType())),
				)
			}
			if err := l.validateCursorTables(columns, schemaName, cursorTableName); err != nil {
				l.logger.Error("cursor table validation failed", zap.String("table_name", tableName), zap.Error(err))
				return fmt.Errorf("invalid cursors table: %w", err)
			}

			l.logger.Info("cursor table validated successfully", zap.String("table_name", tableName))
			seenCursorTable = true
		}
		if tableName == historyTableName {
			l.logger.Debug("found history table", zap.String("table_name", tableName))
			seenHistoryTable = true
		}

		columnByName := make(map[string]*ColumnInfo, len(columns))
		for _, f := range columns {
			columnByName[f.Name()] = &ColumnInfo{
				name:             f.Name(),
				escapedName:      EscapeIdentifier(f.Name()),
				databaseTypeName: f.DatabaseTypeName(),
				scanType:         f.ScanType(),
			}
		}

		key, err := schema.PrimaryKey(l.DB, schemaName, tableName)
		if err != nil {
			return fmt.Errorf("get primary key: %w", err)
		}

		l.tables[tableName], err = NewTableInfo(schemaName, tableName, key, columnByName)
		if err != nil {
			l.logger.Error("failed to create TableInfo", zap.String("table_name", tableName), zap.Error(err))
			return fmt.Errorf("invalid table: %w", err)
		}
		l.logger.Debug("registered table", zap.String("table_name", tableName), zap.Int("column_count", len(columns)))
	}

	if !seenCursorTable {
		return &SystemTableError{fmt.Errorf(`%s.%s table is not found`, EscapeIdentifier(schemaName), cursorTableName)}
	}
	if l.handleReorgs && !seenHistoryTable {
		return &SystemTableError{fmt.Errorf("%s.%s table is not found and reorgs handling is enabled", EscapeIdentifier(schemaName), historyTableName)}
	}

	l.cursorTable = l.tables[cursorTableName]

	l.logger.Info("finished LoadTables",
		zap.Bool("seen_cursor_table", seenCursorTable),
		zap.Bool("seen_history_table", seenHistoryTable),
		zap.Int("registered_table_count", len(l.tables)),
	)

	return nil
}

func (l *Loader) validateCursorTables(columns []*sql.ColumnType, schemaName string, cursorTableName string) (err error) {
	if len(columns) != 4 {
		return &SystemTableError{fmt.Errorf("table requires 4 columns ('id', 'cursor', 'block_num', 'block_id')")}
	}
	columnsCheck := map[string]string{
		"block_num": "int64",
		"block_id":  "string",
		"cursor":    "string",
		"id":        "string",
	}
	for _, f := range columns {
		columnName := f.Name()
		if _, found := columnsCheck[columnName]; !found {
			return &SystemTableError{fmt.Errorf("unexpected column %q in cursors table", columnName)}
		}
		expectedType := columnsCheck[columnName]
		actualType := f.ScanType().Kind().String()
		if expectedType != actualType {
			return &SystemTableError{fmt.Errorf("column %q has invalid type, expected %q has %q", columnName, expectedType, actualType)}
		}
		delete(columnsCheck, columnName)
	}
	if len(columnsCheck) != 0 {
		for k := range columnsCheck {
			return &SystemTableError{fmt.Errorf("missing column %q from cursors", k)}
		}
	}
	key, err := schema.PrimaryKey(l.DB, schemaName, cursorTableName)
	if err != nil {
		return &SystemTableError{fmt.Errorf("failed getting primary key: %w", err)}
	}
	if len(key) == 0 {
		return &SystemTableError{fmt.Errorf("primary key not found: %w", err)}
	}
	if key[0] != "id" {
		return &SystemTableError{fmt.Errorf("column 'id' should be primary key not %q", key[0])}
	}
	return nil
}

func (l *Loader) GetColumnsForTable(name string) []string {
	columns := make([]string, 0, len(l.tables[name].columnsByName))
	for column := range l.tables[name].columnsByName {
		// check if column is empty
		if len(column) > 0 {
			columns = append(columns, column)
		}
	}
	return columns
}

func (l *Loader) GetAvailableTablesInSchema() []string {
	tables := make([]string, len(l.tables))
	i := 0
	for table := range l.tables {
		tables[i] = table
		i++
	}
	return tables
}

func (l *Loader) HasTable(tableName string) bool {
	if _, found := l.tables[tableName]; found {
		return true
	}
	return false
}

func (l *Loader) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	encoder.AddUint64("entries_count", l.entriesCount)
	return nil
}

// Setup creates the schemaName, cursors and history table where the <schemaBytes> is a byte array
// taken from somewhere.
func (l *Loader) Setup(ctx context.Context, schemaName string, userSql string, withPostgraphile bool) error {
	if userSql != "" {
		if err := l.dialect.ExecuteSetupScript(ctx, l, userSql); err != nil {
			return fmt.Errorf("exec userSql: %w", err)
		}
	}

	if err := l.setupCursorTable(ctx, schemaName, withPostgraphile); err != nil {
		return fmt.Errorf("setup cursor table: %w", err)
	}

	if err := l.setupHistoryTable(ctx, schemaName, withPostgraphile); err != nil {
		return fmt.Errorf("setup history table: %w", err)
	}

	return nil
}

func (l *Loader) setupCursorTable(ctx context.Context, schemaName string, withPostgraphile bool) error {
	query := l.dialect.GetCreateCursorQuery(schemaName, withPostgraphile)
	_, err := l.ExecContext(ctx, query)
	return err
}

func (l *Loader) setupHistoryTable(ctx context.Context, schemaName string, withPostgraphile bool) error {
	if l.dialect.OnlyInserts() {
		return nil
	}
	query := l.dialect.GetCreateHistoryQuery(schemaName, withPostgraphile)
	_, err := l.ExecContext(ctx, query)
	return err
}

// GetIdentifier returns <database>/<schema> suitable for user presentation
func (l *Loader) GetIdentifier() string {
	return fmt.Sprintf("%s/%s", l.dsn.schema, l.dsn.schema)
}

type obfuscatedString string

func (s obfuscatedString) String() string {
	if len(s) == 0 {
		return "<unset>"
	}

	return "********"
}
