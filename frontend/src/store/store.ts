import { create } from 'zustand';
import { User, Product, AutocompleteSuggestion, SearchResponse } from '../types';
import { sendEvent } from '../api';

interface StoreState {
  // User state
  user: User | null;
  setUser: (user: User) => void;
  logout: () => void;
  // User history and behavioral
  viewedCategories: string[]; 
  bouncedCategories: string[]; // Penalized categories
  productOpenTimes: Record<string, number>; // productId -> timestamp of opening
  cartProductIds: string[];

  // Search Context
  searchQuery: string;
  results: Product[];
  isSearching: boolean;
  suggestions: AutocompleteSuggestion[];
  correctedQuery: string | null;
  totalFound: number;
  hasMore: boolean;
  searchOffset: number;
  searchLimit: number;
  minScore: number;

  setSearchQuery: (query: string) => void;
  setIsSearching: (isSearching: boolean) => void;
  setSuggestions: (suggestions: AutocompleteSuggestion[]) => void;
  setCorrectedQuery: (query: string | null) => void;
  setSearchResponse: (response: SearchResponse, append?: boolean) => void;
  resetSearchResults: () => void;

  // Actions
  trackProductParams: (category: string) => void;
  simulateProductOpen: (productId: string, category: string) => void;
  simulateProductClose: (productId: string, category: string) => void;
  addToCart: (productId: string, category: string) => void;
}

export const useStore = create<StoreState>((set, get) => ({
  user: null,
  setUser: (user) =>
    set({
      user,
      viewedCategories: user.viewedCategories ?? [],
      bouncedCategories: [],
      productOpenTimes: {},
      cartProductIds: [],
    }),
  logout: () =>
    set({
      user: null,
      viewedCategories: [],
      bouncedCategories: [],
      productOpenTimes: {},
      cartProductIds: [],
      results: [],
      searchQuery: '',
      suggestions: [],
      correctedQuery: null,
      totalFound: 0,
      hasMore: false,
      searchOffset: 0,
    }),

  viewedCategories: [],
  bouncedCategories: [],
  productOpenTimes: {},
  cartProductIds: [],

  searchQuery: '',
  results: [],
  isSearching: false,
  suggestions: [],
  correctedQuery: null,
  totalFound: 0,
  hasMore: false,
  searchOffset: 0,
  searchLimit: 20,
  minScore: 0.55,

  setSearchQuery: (query) => set({ searchQuery: query }),
  setIsSearching: (isSearching) => set({ isSearching }),
  setSuggestions: (suggestions) => set({ suggestions }),
  setCorrectedQuery: (query) => set({ correctedQuery: query }),
  setSearchResponse: (response, append = false) =>
    set((state) => {
      const nextResults = append ? [...state.results, ...response.items] : response.items;
      const totalFound = response.total_found ?? response.totalCount ?? nextResults.length;
      return {
        results: nextResults,
        totalFound,
        hasMore: response.has_more,
        searchOffset: nextResults.length,
      };
    }),
  resetSearchResults: () =>
    set({
      results: [],
      totalFound: 0,
      hasMore: false,
      searchOffset: 0,
    }),

  trackProductParams: (category) =>
    set((state) => ({
      viewedCategories: [...new Set([...state.viewedCategories, category])]
    })),

  simulateProductOpen: (productId, category) => {
    const { user } = get();
    set((state) => ({
      productOpenTimes: { ...state.productOpenTimes, [productId]: Date.now() },
      viewedCategories: [...new Set([...state.viewedCategories, category])]
    }));
    void sendEvent({
      userId: user?.id,
      inn: user?.inn,
      region: user?.region,
      eventType: 'item_open',
      steId: productId,
      category,
    }).catch((error) => console.error(error));
  },

  simulateProductClose: (productId, category) => {
    const { user, productOpenTimes } = get();
    const openTime = productOpenTimes[productId];
    const timeSpent = openTime ? Date.now() - openTime : 10000;

    set((state) => {
      const nextOpenTimes = { ...state.productOpenTimes };
      delete nextOpenTimes[productId];
      return {
        productOpenTimes: nextOpenTimes,
        bouncedCategories:
          timeSpent < 3000
            ? [...new Set([...state.bouncedCategories, category])]
            : state.bouncedCategories,
      };
    });

    void sendEvent({
      userId: user?.id,
      inn: user?.inn,
      region: user?.region,
      eventType: 'item_close',
      steId: productId,
      category,
      durationMs: timeSpent,
    }).catch((error) => console.error(error));
  },

  addToCart: (productId, category) => {
    const { user } = get();
    set((state) => ({
      cartProductIds: [...new Set([...state.cartProductIds, productId])],
      viewedCategories: [...new Set([...state.viewedCategories, category])],
    }));
    void sendEvent({
      userId: user?.id,
      inn: user?.inn,
      region: user?.region,
      eventType: 'cart_add',
      steId: productId,
      category,
    }).catch((error) => console.error(error));
  },
}));
