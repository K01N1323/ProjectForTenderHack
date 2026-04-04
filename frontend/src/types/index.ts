export interface PurchaseCategory {
    category: string;
    purchaseCount: number;
    totalAmount: number;
}

export interface FrequentProduct {
    steId: string;
    name: string;
    category: string;
    purchaseCount: number;
    totalAmount: number;
}

export interface User {
    id: string;
    inn: string;
    region: string;
    viewedCategories: string[];
    topCategories?: PurchaseCategory[];
    frequentProducts?: FrequentProduct[];
}

export interface Product {
    id: string;
    name: string;
    category: string;
    price: number;
    offerCount: number;
    supplierInn: string;
    descriptionPreview?: string;
    reasonToShow?: string; 
}

export interface SearchResponse {
    items: Product[];
    totalCount: number;
    total_found: number;
    has_more: boolean;
    correctedQuery?: string; // Если была опечатка
}

export interface AutocompleteSuggestion {
    text: string;
    type: 'product' | 'category' | 'correction' | 'query';
    reason?: string;
    score: number;
}
