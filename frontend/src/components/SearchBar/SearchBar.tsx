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
            className={`relative w-full mx-auto ${hasFloatingHelper ? 'pb-16 md:pb-20' : ''}`}
        >
            {/* Input Form */}
            <form 
                onSubmit={handleSearchSubmit} 
                className="flex items-stretch relative z-20"
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
                    className="flex-grow outline-none border-none text-base text-gray-900 px-5 py-3.5 bg-[#ecedf0] placeholder-gray-500"
                />
                <button 
                    type="submit" 
                    className="bg-[#da291c] hover:bg-red-700 text-white w-12 flex items-center justify-center transition-colors flex-shrink-0"
                >
                    <Search size={20} />
                </button>
            </form>

            {/* Suggestions Dropdown */}
            {isFocused && suggestions.length > 0 && (
                <div className="absolute top-full left-0 w-full bg-white shadow-lg border border-gray-200 z-10 overflow-hidden py-1 hidden md:block">
                    {suggestions.map((suggestion, idx) => (
                        <div 
                            key={idx}
                            onMouseDown={(e) => {
                                e.preventDefault();
                                handleSuggestionClick(suggestion);
                            }}
                            className="px-5 py-2.5 cursor-pointer hover:bg-gray-100 flex items-center gap-3 text-sm text-gray-800 transition-colors"
                        >
                            <Search size={14} className="text-gray-400" />
                            {suggestion}
                        </div>
                    ))}
                </div>
            )}

            {/* "Did you mean.. / Возможно, вы искали" typo block */}
            {correctedQuery && !isFocused && (
                <div className="mt-3 text-gray-700 font-medium z-10 text-sm">
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
