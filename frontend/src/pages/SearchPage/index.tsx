import React from 'react';
import SearchBar from '../../components/SearchBar/SearchBar';
import ProductCard from '../../components/ProductCard';
import { useStore } from '../../store/store';
import { Loader2 } from 'lucide-react';

const SearchPage = () => {
    const { results, isSearching, searchQuery, correctedQuery } = useStore();

    const hasNoResults = results.length === 0 && searchQuery.length > 0 && !isSearching;
    const isPristine = results.length === 0 && searchQuery.length === 0 && !isSearching && !correctedQuery;

    return (
        <div className="w-full max-w-[1600px] mx-auto px-8 lg:px-12 pb-20">
            <div className="relative z-30 -mx-8 lg:-mx-12 bg-[#f2f4f7] border-b border-[#eceef1] px-8 lg:px-12 py-7">
                <div className="w-full">
                    <SearchBar />
                </div>
            </div>

            <div className="w-full relative z-10 mt-10">
                {isSearching && (
                    <div className="flex flex-col items-center justify-center py-32 text-gray-500">
                        <Loader2 className="animate-spin mb-6 text-[#E03F3F]" size={48} />
                        <p className="text-lg font-medium">Интеллектуальный поиск по каталогу...</p>
                    </div>
                )}

                {!isSearching && isPristine && (
                    <div className="min-h-[calc(100vh-260px)] flex items-center justify-center text-center py-24 text-gray-500">
                        <div>
                            <p className="text-[22px] md:text-[24px] font-semibold text-[#49515e]">
                                Начните поиск, введя название товара или категорию.
                            </p>
                            <p className="mt-3 text-[18px] text-[#9ba1ac]">Например: "Бумага А4 Снегурочка"</p>
                        </div>
                    </div>
                )}

                {!isSearching && hasNoResults && (
                    <div className="text-center py-24 text-gray-600 bg-white border border-[#e6e8ec] shadow-sm">
                        <p className="text-2xl font-bold mb-3 text-gray-900">Ничего не найдено</p>
                        <p className="text-lg text-gray-500">Убедитесь, что запрос написан без ошибок, или попробуйте использовать другие синонимы.</p>
                    </div>
                )}

                {!isSearching && results.length > 0 && (
                    <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4 gap-6">
                        {results.map((product) => (
                            <ProductCard key={product.id} product={product} />
                        ))}
                    </div>
                )}
            </div>
        </div>
    );
};

export default SearchPage;
