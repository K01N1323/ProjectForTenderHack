import { Product, SearchResponse, User } from '../types';

// Моковые данные для автокомплита и поиска
const MOCK_PRODUCTS: Product[] = [
    { id: '1', name: 'Бумага офисная А4 Снегурочка, 500 листов', category: 'Канцелярия', price: 300, supplierInn: '1234567890' },
    { id: '2', name: 'Бумага SvetoCopy А4 500л', category: 'Канцелярия', price: 280, supplierInn: '0987654321' },
    { id: '3', name: 'Ноутбук Lenovo ThinkPad T14', category: 'Компьютеры', price: 120000, supplierInn: '5555555555' },
    { id: '4', name: 'Ноутбук Apple MacBook Air 13', category: 'Компьютеры', price: 150000, supplierInn: '7777777777' },
    { id: '5', name: 'Стул офисный ИКЕА Маркус', category: 'Мебель', price: 12000, supplierInn: '1111111111' },
    { id: '6', name: 'Стол рабочий угловой 160см', category: 'Мебель', price: 15000, supplierInn: '1111111111' },
    { id: '7', name: 'Принтер лазерный HP LaserJet', category: 'Оргтехника', price: 25000, supplierInn: '2222222222' },
];

export const searchProducts = async (
    query: string,
    viewedCategories: string[],
    bouncedCategories: string[],
    region: string = 'Moscow'
): Promise<SearchResponse> => {
    return new Promise((resolve) => {
        setTimeout(() => {
            let searchQuery = query.toLowerCase();
            let correctedQuery = undefined;

            // 1. Имитация опечатки
            if (searchQuery === 'бумвга') {
                searchQuery = 'бумага';
                correctedQuery = 'бумага';
            } else if (searchQuery === 'компютер') {
                searchQuery = 'компьютер';
                correctedQuery = 'компьютер';
            }

            if (!searchQuery.trim()) {
                resolve({ items: [], totalCount: 0 });
                return;
            }

            // Фильтрация товаров по поиску
            let matchedProducts = MOCK_PRODUCTS.filter(p =>
                p.name.toLowerCase().includes(searchQuery) ||
                p.category.toLowerCase().includes(searchQuery)
            );

            // Обработка логики персонализации и пессимизации
            let finalProducts = matchedProducts.map(product => {
                let p = { ...product };
                let score = 0;

                // Если категория находится в пессимизированных ("Киллер-фича" быстрый возврат)
                if (bouncedCategories.includes(p.category)) {
                    score -= 100;
                    // Мы не будем ставить reasonToShow, просто опустим в выдаче.
                } 
                // Премирование за ранее просмотренные категории
                else if (viewedCategories.includes(p.category)) {
                    score += 50;
                    p.reasonToShow = 'На основе ваших закупок';
                }

                // Имитация региональной привязки (например HP от поставщика 222 всегда релевантен Москве)
                if (p.supplierInn === '2222222222' && region === 'Moscow') {
                    score += 30;
                    p.reasonToShow = 'Релевантно вашему региону';
                }

                return { product: p, score };
            });

            // Сортировка - сначала премированные, потом обычные, потом пессимизированные
            finalProducts.sort((a, b) => b.score - a.score);

            resolve({
                items: finalProducts.map(f => f.product),
                totalCount: finalProducts.length,
                correctedQuery
            });
        }, 500); // 500ms delay
    });
};

export const getSuggestions = async (query: string): Promise<string[]> => {
    return new Promise((resolve) => {
        setTimeout(() => {
            if (!query.trim()) {
                resolve([]);
                return;
            }
            const lq = query.toLowerCase();
            // Возвращаем категории или названия как подсказки
            const suggestions = MOCK_PRODUCTS
                .filter(p => p.name.toLowerCase().includes(lq) || p.category.toLowerCase().includes(lq))
                .map(p => p.name)
                .slice(0, 5); // Макс 5 подсказок
            
            // Если массив пустой, предложим что-то рандомное для "умного" вида
            if (suggestions.length === 0 && lq.startsWith('бум')) {
                resolve(['бумага А4', 'бумага писчая', 'бумажные полотенца']);
            } else {
                resolve(suggestions);
            }
        }, 200);
    });
};

export const login = async (inn: string): Promise<User> => {
    return new Promise((resolve) => {
        setTimeout(() => {
            resolve({
                id: Math.random().toString(36).substr(2, 9),
                inn: inn,
                region: 'Moscow', // Mock region
                viewedCategories: []
            });
        }, 500);
    });
};
