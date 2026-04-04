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
            className={`relative w-full max-w-4xl mx-auto ${hasFloatingHelper ? 'pb-16 md:pb-20' : ''}`}
        >
            {/* Input Form */}
            <form 
                onSubmit={handleSearchSubmit} 
                className="flex items-center bg-white shadow-xl rounded-full p-2 pl-6 relative z-20 border border-transparent focus-within:border-gray-200 transition-all"
            >
                <Search size={28} className="text-gray-400" />
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
                    placeholder="Поиск по классификатору СТЕ..."
                    className="flex-grow outline-none border-none text-xl text-gray-900 px-6 py-4 bg-transparent placeholder-gray-400"
                />
                <button 
                    type="submit" 
                    className="bg-[#E03F3F] hover:bg-red-700 text-white px-10 py-4 rounded-full font-bold text-lg transition-colors flex-shrink-0"
                >
                    Найти
                </button>
            </form>

            {/* Suggestions Dropdown */}
            {isFocused && suggestions.length > 0 && (
                <div className="absolute top-[80px] left-0 w-full bg-white shadow-lg rounded-xl border border-gray-100 mt-2 z-10 overflow-hidden py-2 hidden md:block">
                    {suggestions.map((suggestion, idx) => (
                        <div 
                            key={idx}
                            onMouseDown={(e) => {
                                e.preventDefault();
                                handleSuggestionClick(suggestion);
                            }}
                            className="px-6 py-3 cursor-pointer hover:bg-gray-100 flex items-center gap-3 text-lg text-gray-800 transition-colors"
                        >
                            <Search size={18} className="text-gray-400" />
                            {suggestion}
                        </div>
                    ))}
                </div>
            )}

            {/* "Did you mean.. / Возможно, вы искали" typo block */}
            {correctedQuery && !isFocused && (
                <div className="absolute top-[90px] left-8 mt-2 text-gray-700 font-medium z-10 text-lg">
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
