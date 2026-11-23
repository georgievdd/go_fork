package app

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	"example.com/order-pipeline/internal/catalog"
	"example.com/order-pipeline/internal/checks"
	"example.com/order-pipeline/pkg/model"
	"example.com/order-pipeline/pkg/orderclient"
	"example.com/order-pipeline/pkg/supplier"
)

const usageText = `Использование:

  order-report -products <каталог.csv> -supplies <поставки.csv> -orders <заказы.csv> -supplier-status <статусы.csv>

Все четыре флага обязательны и поддерживают формы -name value и -name=value
в любом порядке. Позиционных аргументов нет.

Список путей к CSV уведомлений поставщиков читается из stdin по одному пути
на строку. Пустые строки пропускаются.

События проверки выводятся в stdout, диагностика ошибок — в stderr.
Код завершения: 0 - успех, 1 - ошибка stdin/stdout, проверки или сохранения
заказа, 2 — неверные аргументы, недоступный файл, плохой CSV или неверные
ссылки в данных.
`

type options struct {
	products string
	supplies string
	orders   string
	status   string
}

func Run(stdin io.Reader, stdout, stderr io.Writer, args []string) int {
	opts, err := parseArgs(args)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprint(stdout, usageText)
			return 0
		}
		fmt.Fprintln(stderr, "order-report:", err)
		return 2
	}
	paths, err := readNoticePaths(stdin)
	if err != nil {
		fmt.Fprintln(stderr, "order-report:", err)
		return 1
	}
	pairs, candidates, clients, err := load(opts, paths)
	if err != nil {
		fmt.Fprintln(stderr, "order-report:", err)
		return 2
	}
	if err := checks.Run(checks.Deps{
		Orders:   candidates,
		Pairs:    pairs,
		Supplier: clients.supplier,
		Client:   clients.orders,
		Out:      stdout,
	}); err != nil {
		fmt.Fprintln(stderr, "order-report:", err)
		return 1
	}
	return 0
}

func parseArgs(args []string) (options, error) {
	fs := flag.NewFlagSet("order-report", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var opts options
	fs.StringVar(&opts.products, "products", "", "путь к CSV каталога товаров")
	fs.StringVar(&opts.supplies, "supplies", "", "путь к CSV состава поставок")
	fs.StringVar(&opts.orders, "orders", "", "путь к CSV заказов")
	fs.StringVar(&opts.status, "supplier-status", "", "путь к CSV ответов поставщика")
	if err := fs.Parse(args); err != nil {
		return opts, err
	}
	if fs.NArg() > 0 {
		return opts, fmt.Errorf("лишние позиционные аргументы: %s", strings.Join(fs.Args(), " "))
	}
	names := map[string]*string{
		"-products":        &opts.products,
		"-supplies":        &opts.supplies,
		"-orders":          &opts.orders,
		"-supplier-status": &opts.status,
	}
	for name, value := range names {
		if strings.TrimSpace(*value) == "" {
			return opts, fmt.Errorf("обязательный флаг %s отсутствует или пуст", name)
		}
	}
	return opts, nil
}

func readNoticePaths(stdin io.Reader) ([]string, error) {
	scanner := bufio.NewScanner(stdin)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	var paths []string
	for scanner.Scan() {
		path := strings.TrimSpace(scanner.Text())
		if path != "" {
			paths = append(paths, path)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("чтение списка уведомлений из stdin: %w", err)
	}
	return paths, nil
}

type clients struct {
	orders   *orderclient.Client
	supplier *supplier.Client
}

func load(opts options, paths []string) (map[model.RiskPosition]bool, []model.Order, clients, error) {
	var prepared clients
	ordersClient, err := orderclient.New(opts.orders)
	if err != nil {
		return nil, nil, prepared, err
	}
	supplierClient, err := supplier.New(opts.status)
	if err != nil {
		return nil, nil, prepared, err
	}
	prepared = clients{orders: ordersClient, supplier: supplierClient}
	items, err := catalog.Load(opts.products, opts.supplies)
	if err != nil {
		return nil, nil, prepared, err
	}
	pairs, selected, err := selectNotices(supplierClient, items, paths)
	if err != nil {
		return nil, nil, prepared, err
	}
	orders := ordersClient.Orders()
	for _, order := range orders {
		for _, item := range order.Items {
			if !items.HasProduct(item.ProductID) {
				return nil, nil, prepared, fmt.Errorf("заказ %d: строка %d: неизвестный товар %d", order.ID, item.LineID, item.ProductID)
			}
			if !items.HasPair(item.ProductID, item.SupplyID) {
				return nil, nil, prepared, fmt.Errorf("заказ %d: строка %d: неизвестная пара товар–поставка %d/%d", order.ID, item.LineID, item.ProductID, item.SupplyID)
			}
		}
	}
	return pairs, checks.SelectCandidates(orders, selected), prepared, nil
}

func selectNotices(supplierClient *supplier.Client, items *catalog.Catalog, paths []string) (map[model.RiskPosition]bool, map[uint64]bool, error) {
	pairs := make(map[model.RiskPosition]bool)
	selected := make(map[uint64]bool)
	for _, path := range paths {
		notices, err := supplierClient.ReadNotices(path)
		if err != nil {
			return nil, nil, err
		}
		for _, notice := range notices {
			productID, ok := items.ProductByCode(notice.ProductCode)
			if !ok {
				return nil, nil, fmt.Errorf("%s: неизвестный код товара %q", path, notice.ProductCode)
			}
			if !items.HasPair(productID, notice.SupplyID) {
				return nil, nil, fmt.Errorf("%s: неизвестная пара товар–поставка %d/%d", path, productID, notice.SupplyID)
			}
			pairs[model.RiskPosition{ProductID: productID, SupplyID: notice.SupplyID}] = true
			selected[productID] = true
		}
	}
	return pairs, selected, nil
}
