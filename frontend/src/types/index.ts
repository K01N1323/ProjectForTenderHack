export interface User {
    id: string;
    inn: string;
    region: string;
    viewedCategories: string[];
}

export interface Product {
    id: string;
    name: string;
    category: string;
    price: number;
    supplierInn: string;
    reasonToShow?: string; 
}

export interface SearchResponse {
    items: Product[];
    totalCount: number;
    correctedQuery?: string; // Если была опечатка
}
