import React, { useState, useEffect, useRef } from 'react';
import { Search } from 'lucide-react';
import { useStore } from '../../store/store';
import { searchProducts, getSuggestions } from '../../api';

const SearchBar = () => {
    const { 
        searchQuery, 
        setSearchQuery, 
        setResults, 
        setIsSearching, 
        suggestions, 
        setSuggestions,
        correctedQuery,
        setCorrectedQuery,
        user,
        viewedCategories,
        bouncedCategories
    } = useStore();
    
    const [localQuery, setLocalQuery] = useState(searchQuery);
    const [isFocused, setIsFocused] = useState(false);
    
    // Debounce timer logic
    const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
    const suggestionRequestRef = useRef(0);

    const loadSuggestions = async (queryToSuggest: string) => {
        const trimmedQuery = queryToSuggest.trim();
        const requestId = ++suggestionRequestRef.current;

        if (!trimmedQuery) {
            setSuggestions([]);
            return;
        }

        try {
            const suggs = await getSuggestions(trimmedQuery);
            if (suggestionRequestRef.current === requestId) {
                setSuggestions(suggs);
            }
        } catch (error) {
            console.error(error);
            if (suggestionRequestRef.current === requestId) {
                setSuggestions([]);
            }
        }
    };

    // Executing full search
    const performSearch = async (queryToSearch: string) => {
        setIsSearching(true);
        setSearchQuery(queryToSearch);
        setIsFocused(false); // Close suggestions on full search
        suggestionRequestRef.current += 1;
        setSuggestions([]);
        try {
            const response = await searchProducts(
                queryToSearch, 
                user,
                viewedCategories, 
                bouncedCategories, 
            );
            setResults(response.items);
            setCorrectedQuery(response.correctedQuery || null);
        } catch (error) {
            console.error(error);
        } finally {
            setIsSearching(false);
        }
    };

    // Auto-search via debounce, plus suggestion fetching
    useEffect(() => {
        if (timerRef.current) clearTimeout(timerRef.current);
        
        if (localQuery.trim() === '') {
            setSuggestions([]);
            return;
        }

        timerRef.current = setTimeout(() => {
            void loadSuggestions(localQuery);
        }, 300);

        return () => {
            if (timerRef.current) clearTimeout(timerRef.current);
        };
    }, [localQuery, setSuggestions]);

    const handleSearchSubmit = (e?: React.FormEvent) => {
        if (e) e.preventDefault();
        performSearch(localQuery);
    };

    const handleSuggestionClick = (suggestion: string) => {
        setLocalQuery(suggestion);
        performSearch(suggestion);
    };

    const handleCorrectedQueryClick = () => {
        if (correctedQuery) {
            setLocalQuery(correctedQuery);
            setCorrectedQuery(null);
            performSearch(correctedQuery);
        }
    };

    const hasFloatingHelper = Boolean(correctedQuery && !isFocused);

    return (
        <div
            className={`relative w-full ${hasFloatingHelper ? 'pb-14 md:pb-16' : ''}`}
        >
            <form 
                onSubmit={handleSearchSubmit} 
                className="flex items-stretch bg-[#f1f3f6] border border-[#e6e8ec] relative z-20 transition-colors focus-within:border-[#d4d8de]"
            >
                <input 
                    type="text" 
                    value={localQuery}
                    onChange={(e) => {
                        const nextQuery = e.target.value;
                        setLocalQuery(nextQuery);
                        setIsFocused(Boolean(nextQuery.trim()));
                        if (correctedQuery) {
                            setCorrectedQuery(null);
                        }
                    }}
                    onFocus={() => {
                        setIsFocused(true);
                        if (localQuery.trim()) {
                            void loadSuggestions(localQuery);
                        }
                    }}
                    // Delay blur so click on suggestion can register
                    onBlur={() => setTimeout(() => setIsFocused(false), 200)}
                    placeholder="Введите название категории, товара или ID СТЕ"
                    className="flex-grow outline-none border-none text-[20px] md:text-[22px] text-gray-900 px-7 py-5 bg-transparent placeholder-[#7d8390]"
                />
                <button 
                    type="submit" 
                    className="bg-[#d63d2b] hover:bg-[#bf3324] text-white w-[68px] md:w-[72px] flex items-center justify-center transition-colors flex-shrink-0"
                    aria-label="Запустить поиск"
                >
                    <Search size={30} strokeWidth={2} />
                </button>
            </form>

            {isFocused && suggestions.length > 0 && (
                <div className="absolute top-[76px] left-0 w-full bg-white shadow-lg border border-[#e6e8ec] mt-2 z-20 overflow-hidden py-2 hidden md:block">
                    {suggestions.map((suggestion, idx) => (
                        <div 
                            key={idx}
                            onMouseDown={(e) => {
                                e.preventDefault();
                                handleSuggestionClick(suggestion);
                            }}
                            className="px-6 py-3 cursor-pointer hover:bg-[#f4f6f8] flex items-center gap-3 text-[18px] text-gray-800 transition-colors"
                        >
                            <Search size={18} className="text-gray-400" />
                            {suggestion}
                        </div>
                    ))}
                </div>
            )}

            {correctedQuery && !isFocused && (
                <div className="absolute top-[84px] left-0 mt-2 text-gray-700 font-medium z-10 text-base md:text-lg">
                    Возможно, вы искали: 
                    <span 
                        onClick={handleCorrectedQueryClick}
                        className="ml-2 text-blue-600 underline cursor-pointer hover:text-blue-800"
                    >
                        {correctedQuery}
                    </span>
                </div>
            )}
        </div>
    );
};

export default SearchBar;
