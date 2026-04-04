import axios from 'axios';
import { SearchResponse, User } from '../types';

const api = axios.create({
    baseURL: import.meta.env.VITE_API_BASE_URL || 'http://127.0.0.1:8000',
    timeout: 15000,
});

export interface SearchEventPayload {
    userId?: string;
    inn?: string;
    region?: string;
    eventType:
        | 'search_result_click'
        | 'item_open'
        | 'item_close'
        | 'bounce'
        | 'cart_add'
        | 'cart_remove'
        | 'purchase'
        | 'item_click';
    steId?: string;
    category?: string;
    durationMs?: number;
}

export const searchProducts = async (
    query: string,
    user: User | null,
    viewedCategories: string[],
    bouncedCategories: string[],
): Promise<SearchResponse> => {
    const response = await api.post<SearchResponse>('/api/search', {
        query,
        userContext: user
            ? {
                  id: user.id,
                  inn: user.inn,
                  region: user.region,
                  viewedCategories: user.viewedCategories,
              }
            : null,
        viewedCategories,
        bouncedCategories,
    });
    return response.data;
};

export const getSuggestions = async (query: string): Promise<string[]> => {
    if (!query.trim()) {
        return [];
    }
    const response = await api.get<string[]>('/api/search/suggestions', {
        params: { q: query },
    });
    return response.data;
};

export const login = async (inn: string): Promise<User> => {
    const response = await api.post<User>('/api/auth/login', { inn });
    return response.data;
};

export const sendEvent = async (payload: SearchEventPayload): Promise<void> => {
    await api.post('/api/event', payload);
};
