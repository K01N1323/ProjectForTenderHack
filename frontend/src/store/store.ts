import { create } from 'zustand';
import { User, Product } from '../types';

interface StoreState {
  // User state
  user: User | null;
  setUser: (user: User) => void;
  logout: () => void;
  // User history and behavioral
  viewedCategories: string[]; 
  bouncedCategories: string[]; // Penalized categories
  productOpenTimes: Record<string, number>; // productId -> timestamp of opening

  // Search Context
  searchQuery: string;
  results: Product[];
  isSearching: boolean;
  suggestions: string[];
  correctedQuery: string | null;

  setSearchQuery: (query: string) => void;
  setResults: (results: Product[]) => void;
  setIsSearching: (isSearching: boolean) => void;
  setSuggestions: (suggestions: string[]) => void;
  setCorrectedQuery: (query: string | null) => void;

  // Actions
  trackProductParams: (category: string) => void;
  simulateProductOpen: (productId: string, category: string) => void;
  simulateProductClose: (productId: string, category: string) => void;
}

export const useStore = create<StoreState>((set) => ({
  user: null,
  setUser: (user) =>
    set({
      user,
      viewedCategories: user.viewedCategories ?? [],
      bouncedCategories: [],
      productOpenTimes: {},
    }),
  logout: () =>
    set({
      user: null,
      viewedCategories: [],
      bouncedCategories: [],
      productOpenTimes: {},
      results: [],
      searchQuery: '',
      suggestions: [],
      correctedQuery: null,
    }),

  viewedCategories: [],
  bouncedCategories: [],
  productOpenTimes: {},

  searchQuery: '',
  results: [],
  isSearching: false,
  suggestions: [],
  correctedQuery: null,

  setSearchQuery: (query) => set({ searchQuery: query }),
  setResults: (results) => set({ results }),
  setIsSearching: (isSearching) => set({ isSearching }),
  setSuggestions: (suggestions) => set({ suggestions }),
  setCorrectedQuery: (query) => set({ correctedQuery: query }),

  trackProductParams: (category) =>
    set((state) => ({
      viewedCategories: [...new Set([...state.viewedCategories, category])]
    })),

  simulateProductOpen: (productId, category) =>
    set((state) => ({
      productOpenTimes: { ...state.productOpenTimes, [productId]: Date.now() },
      viewedCategories: [...new Set([...state.viewedCategories, category])]
    })),

  simulateProductClose: (productId, category) =>
    set((state) => {
      const openTime = state.productOpenTimes[productId];
      const timeSpent = openTime ? Date.now() - openTime : 10000;
      
      // Fast bounce threshold (e.g., less than 3 seconds)
      if (timeSpent < 3000) {
        return {
          bouncedCategories: [...new Set([...state.bouncedCategories, category])]
        };
      }
      return state;
    }),
}));
