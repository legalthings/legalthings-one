import { Injectable, Inject, ClassProvider } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Observable, timer } from 'rxjs';
import { map, switchMap, switchMapTo, distinctUntilChanged, catchError } from 'rxjs/operators';
import { LTO_PUBLIC_API } from '../../tokens';
import { transactionsFilter } from '../utils';
import { TransactionTypes } from '../transaction-types';

interface CompildedScript {
  script: string;
  complexity: number;
  extraFee: number;
}

/**
 * Provide communication with LTO backend
 */
@Injectable()
export class PublicNodeImpl implements PublicNode {
  constructor(private _http: HttpClient, @Inject(LTO_PUBLIC_API) private _publicApi: string) {}

  version(): Observable<string> {
    return this._http.get<any>(this._publicApi + 'node/version').pipe(map(data => data.version));
  }

  height(): Observable<number> {
    return this._http.get<any>(this._publicApi + 'blocks/height').pipe(map(data => data.height));
  }

  lastBlocks(
    count: number = 20,
    poll: boolean = false,
    pollInterval: number = 5000
  ): Observable<any> {
    const pollTimer = poll ? timer(0, pollInterval) : timer(0);
    return pollTimer.pipe(
      switchMapTo(this.height()),
      distinctUntilChanged(),
      switchMap(height => this.headerSequence(height, count))
    );
  }

  headerSequence(height: number, count: number): Observable<any[]> {
    let from = height - count + 1; // Exclude last one
    from = from < 0 ? 0 : from; // Prevent height less than zero
    return this._http
      .get<any[]>(this._publicApi + 'blocks/headers/seq/' + from + '/' + height)
      .pipe(map(blocks => blocks.reverse()));
  }

  transaction(id: string): Observable<any> {
    return this._http.get<any>(this._publicApi + 'transactions/info/' + id);
  }

  block(height: number | string): Observable<any> {
    return this._http.get<any>(this._publicApi + 'blocks/at/' + height);
  }

  transactionsOf(address: string): Observable<any> {
    return this._http
      .get<any>(this._publicApi + 'transactions/address/' + address + '/limit/' + 200)
      .pipe(map(response => response[0]));
  }

  indexedTransactions(
    address: string,
    index: string = 'anchor',
    limit = 100
  ): Observable<LTO.Page<LTO.Transaction>> {
    const params: any = {
      limit,
      type: index
    };
    let headers = new HttpHeaders();
    headers = headers.set('Cache-Control', 'no-cache');
    return this._http
      .get<any[]>(this._publicApi + 'index/transactions/addresses/' + address, {
        params,
        headers,
        observe: 'response'
      })
      .pipe(
        map(response => {
          const total = parseInt(response.headers.get('x-total') || '0', 10);
          return {
            total,
            items: response.body || []
          };
        }),
        catchError(() => {
          if (index === 'all_transfers') {
            return this.indexedTransactions(address, 'transfer', limit);
          }

          return this.transactionsOf(address).pipe(
            map(allTransactions => {
              let transactionType: TransactionTypes[] = [];
              if (index === 'anchor') {
                transactionType = [TransactionTypes.ANCHOR];
              } else if (index === 'transfer') {
                transactionType = [TransactionTypes.TRANSFER, TransactionTypes.MASS_TRANSFER];
              }

              const transactions = transactionsFilter(...transactionType)(allTransactions);
              return {
                total: transactions.length,
                items: transactions
              };
            })
          );
        })
      );
  }

  balanceOf(address: string): Observable<any> {
    return this._http.get<any>(this._publicApi + 'addresses/balance/details/' + address);
  }

  unconfirmedTransactions(): Observable<any[]> {
    return this._http.get<any>(this._publicApi + 'transactions/unconfirmed');
  }

  activeLease(address: string): Observable<LTO.Transaction[]> {
    return this._http.get<LTO.Transaction[]>(`${this._publicApi}leasing/active/${address}`);
  }

  getScript(address: string): Observable<any> {
    return this._http.get<any>(`${this._publicApi}addresses/scriptInfo/${address}`);
  }

  compileScript(code: string): Observable<CompildedScript> {
    return this._http.post<CompildedScript>(`${this._publicApi}utils/script/compile`, code);
  }
}

export abstract class PublicNode {
  static provider: ClassProvider = {
    provide: PublicNode,
    useClass: PublicNodeImpl
  };

  abstract version(): Observable<string>;
  abstract height(): Observable<number>;
  abstract lastBlocks(count: number, poll: boolean, pollInterval: number): Observable<any[]>;
  abstract headerSequence(height: number, count: number): Observable<any[]>;
  abstract transaction(id: string): Observable<any>;
  abstract block(height: number | string): Observable<any>;
  abstract transactionsOf(address: string): Observable<LTO.Transaction[]>;
  abstract indexedTransactions(
    address: string,
    index?: string,
    limit?: number
  ): Observable<LTO.Page<LTO.Transaction>>;
  abstract balanceOf(address: string): Observable<any>;
  abstract unconfirmedTransactions(): Observable<any[]>;
  abstract activeLease(address: string): Observable<LTO.Transaction[]>;
  abstract getScript(address: string): Observable<any>;
  abstract compileScript(code: string): Observable<CompildedScript>;
}
