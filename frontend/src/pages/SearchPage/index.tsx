import React, { useState } from 'react';
import SearchBar from '../../components/SearchBar/SearchBar';
import ProductCard from '../../components/ProductCard';
import { useStore } from '../../store/store';
import { Loader2, Search } from 'lucide-react';
import { searchProducts } from '../../api';

const SearchPage = () => {
    const {
        results,
        isSearching,
        searchQuery,
        correctedQuery,
        totalFound,
        hasMore,
        searchOffset,
        searchLimit,
        minScore,
        user,
        viewedCategories,
        bouncedCategories,
        setSearchResponse,
    } = useStore();
    const [isLoadingMore, setIsLoadingMore] = useState(false);

    const hasNoResults = results.length === 0 && searchQuery.length > 0 && !isSearching;
    const isPristine = results.length === 0 && searchQuery.length === 0 && !isSearching && !correctedQuery;
    const shouldShowSummary = Boolean(searchQuery.trim()) && !isPristine && !isSearching;
    const showInitialLoader = isSearching && results.length === 0;

    const handleLoadMore = async () => {
        if (!searchQuery.trim() || !hasMore || isLoadingMore) {
            return;
        }

        setIsLoadingMore(true);
        try {
            const response = await searchProducts(
                searchQuery,
                user,
                viewedCategories,
                bouncedCategories,
                {
                    limit: searchLimit,
                    offset: searchOffset,
                    minScore,
                },
            );
            setSearchResponse(response, true);
        } catch (error) {
            console.error(error);
        } finally {
            setIsLoadingMore(false);
        }
    };

    return (
        <div className="w-full max-w-[1600px] mx-auto px-8 lg:px-12 pb-20">
            <div className="relative z-30 -mx-8 lg:-mx-12 bg-[#f2f4f7] border-b border-[#eceef1] px-8 lg:px-12 py-7">
                <div className="w-full">
                    <SearchBar />
                </div>
            </div>

            <div className="w-full relative z-10 mt-10">
                {showInitialLoader && (
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

                {shouldShowSummary && (
                    <div className="mb-6 rounded-2xl border border-[#e6e8ec] bg-white px-5 py-4 text-[17px] font-medium text-[#49515e] shadow-sm">
                        Найдено предложений: {totalFound}
                    </div>
                )}

                {!isSearching && hasNoResults && (
                    <div className="rounded-[28px] border border-[#e6e8ec] bg-[linear-gradient(135deg,#ffffff_0%,#f8fafc_100%)] px-8 py-20 text-center text-gray-600 shadow-sm">
                        <div className="mx-auto flex h-20 w-20 items-center justify-center rounded-full bg-[#f3f5f8] text-[#E03F3F]">
                            <Search size={34} strokeWidth={2.2} />
                        </div>
                        <p className="mt-6 text-[28px] font-semibold text-[#1f2937]">
                            По вашему запросу ничего не найдено
                        </p>
                        <p className="mx-auto mt-3 max-w-2xl text-[18px] leading-8 text-[#7b8494]">
                            Попробуйте изменить формулировку
                        </p>
                    </div>
                )}

                {!isSearching && results.length > 0 && (
                    <div>
                        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4 gap-6">
                            {results.map((product) => (
                                <ProductCard key={product.id} product={product} />
                            ))}
                        </div>

                        {hasMore && (
                            <div className="mt-10 flex justify-center">
                                <button
                                    type="button"
                                    onClick={() => void handleLoadMore()}
                                    disabled={isLoadingMore}
                                    className="inline-flex min-w-[220px] items-center justify-center gap-3 rounded-full border border-[#d9dee5] bg-white px-8 py-4 text-[17px] font-medium text-[#303846] shadow-sm transition-colors hover:border-[#c7ced8] hover:bg-[#f7f9fb] disabled:cursor-not-allowed disabled:opacity-70"
                                >
                                    {isLoadingMore && <Loader2 className="animate-spin" size={18} />}
                                    {isLoadingMore ? 'Загружаем еще...' : 'Показать еще'}
                                </button>
                            </div>
                        )}
                    </div>
                )}
            </div>
        </div>
    );
};

export default SearchPage;
