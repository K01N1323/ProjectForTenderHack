import { create } from 'zustand';
import { User, Product } from '../types';
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
  cartProducts: Product[];

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
  addToCart: (product: Product) => void;
  removeFromCart: (productId: string) => void;
  clearCart: () => void;
}

export const useStore = create<StoreState>((set, get) => ({
  user: null,
  setUser: (user) =>
    set({
      user,
      viewedCategories: user.viewedCategories ?? [],
      bouncedCategories: [],
      productOpenTimes: {},
      cartProducts: [],
    }),
  logout: () =>
    set({
      user: null,
      viewedCategories: [],
      bouncedCategories: [],
      productOpenTimes: {},
      cartProducts: [],
      results: [],
      searchQuery: '',
      suggestions: [],
      correctedQuery: null,
    }),

  viewedCategories: [],
  bouncedCategories: [],
  productOpenTimes: {},
  cartProducts: [],

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
          timeSpent < 2000
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

  addToCart: (product) => {
    const { user } = get();
    set((state) => ({
      cartProducts: state.cartProducts.some(p => p.id === product.id) 
        ? state.cartProducts 
        : [...state.cartProducts, product],
      viewedCategories: [...new Set([...state.viewedCategories, product.category])],
    }));
    void sendEvent({
      userId: user?.id,
      inn: user?.inn,
      region: user?.region,
      eventType: 'cart_add',
      steId: product.id,
      category: product.category,
    }).catch((error) => console.error(error));
  },

  removeFromCart: (productId) => {
    const { user, cartProducts } = get();
    const productToRemove = cartProducts.find(p => p.id === productId);
    set((state) => ({
      cartProducts: state.cartProducts.filter(p => p.id !== productId),
    }));
    if (productToRemove) {
      void sendEvent({
        userId: user?.id,
        inn: user?.inn,
        region: user?.region,
        eventType: 'cart_remove',
        steId: productId,
        category: productToRemove.category,
      }).catch((error) => console.error(error));
    }
  },

  clearCart: () => {
    const { user, cartProducts } = get();
    // Simulate purchase events for all products in cart
    cartProducts.forEach(product => {
      void sendEvent({
        userId: user?.id,
        inn: user?.inn,
        region: user?.region,
        eventType: 'purchase',
        steId: product.id,
        category: product.category,
      }).catch((error) => console.error(error));
    });
    set({ cartProducts: [] });
  },
}));
