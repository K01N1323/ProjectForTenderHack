import React from 'react';

const SearchBar = () => {
    return (
        <div className="search-bar">
            <input 
                type="text" 
                placeholder="Поиск по СТЕ (например, 'бумага офисная')" 
            />
            {/* Autocomplete dropdown here */}
        </div>
    );
};

export default SearchBar;
