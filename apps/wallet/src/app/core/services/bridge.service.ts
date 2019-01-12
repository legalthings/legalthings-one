import { Injectable, Inject, ClassProvider } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, of } from 'rxjs';
import { map, tap, shareReplay } from 'rxjs/operators';
import { LTO_BRIDGE_HOST, BRIDGE_ENABLED } from '../../tokens';

type TokenType = 'LTO' | 'LTO20' | 'WAVES';

interface BridgeCache {
  deposit: {
    [address: string]: string;
  };

  withdraw: {
    [recipient: string]: string;
  };
}

interface BridgeStats {
  burn_rate: number;
  burned: number;
}

@Injectable()
export class BridgeServiceImpl implements BridgeService {
  readonly STORAGE_KEY = '__bridge__';

  burnRate$: Observable<number>;

  bridgeStats$: Observable<BridgeStats>;
  private cache: BridgeCache;

  constructor(@Inject(LTO_BRIDGE_HOST) private ltoBridgeHost: string, private http: HttpClient) {
    // Restore bridge address from localstorage
    this.cache = this.restoreCache();

    this.bridgeStats$ = http.get<BridgeStats>(`${this.ltoBridgeHost}/stats`).pipe(shareReplay(1));
    this.burnRate$ = this.bridgeStats$.pipe(map(stats => stats.burn_rate));

    // Make it hot
    this.bridgeStats$.subscribe();
  }

  depositTo(address: string, captcha: string, tokenType: TokenType = 'LTO20'): Observable<string> {
    if (this.cache.deposit[address]) {
      return of(this.cache.deposit[address]);
    }

    return this.createBridgeAddress(tokenType, 'LTO', address, captcha).pipe(
      tap(bridge => {
        this.cache.deposit[address] = bridge;
        this.saveCache(this.cache);
      })
    );
  }

  withdrawTo(recipient: string, captcha: string): Observable<string> {
    if (this.cache.withdraw[recipient]) {
      return of(this.cache.withdraw[recipient]);
    }

    return this.createBridgeAddress('LTO', 'LTO20', recipient, captcha).pipe(
      tap(bridge => {
        this.cache.withdraw[recipient] = bridge;
        this.saveCache(this.cache);
      })
    );
  }

  private createBridgeAddress(
    fromToken: TokenType,
    toToken: TokenType,
    toAddress: string,
    captcha: string
  ): Observable<string> {
    return this.http
      .post<any>(this.ltoBridgeHost + '/bridge/address', {
        from_token: fromToken,
        to_token: toToken,
        to_address: toAddress,
        captcha_response: captcha
      })
      .pipe(map(response => response.address));
  }

  private restoreCache(): BridgeCache {
    const cache = localStorage.getItem(this.STORAGE_KEY);
    if (cache) {
      return JSON.parse(cache);
    }

    const initialCache: BridgeCache = {
      deposit: {},
      withdraw: {}
    };
    this.saveCache(initialCache);

    return initialCache;
  }

  private saveCache(cache: BridgeCache) {
    localStorage.setItem(this.STORAGE_KEY, JSON.stringify(cache));
  }
}

export class BridgeServiceDummy implements BridgeService {
  burnRate$ = of(0);

  depositTo(): Observable<string> {
    return of('BRIDGE_IS_DISABLED');
  }

  withdrawTo(): Observable<string> {
    return of('BRIDGE_IS_DISABLED');
  }
}

export abstract class BridgeService {
  static provider: ClassProvider = {
    provide: BridgeService,
    useClass: BridgeServiceImpl
  };

  abstract burnRate$: Observable<number>;

  /**
   * Generates bridge addres to convert LTO24 -> LTO and transfer on your account
   * @param address - your account address
   */
  abstract depositTo(address: string, captcha: string, tokenType?: TokenType): Observable<string>;

  /**
   * Generate bridge addres to convert LTO -> LTO20
   * @param address - recipient addres
   */
  abstract withdrawTo(recipient: string, captcha: string): Observable<string>;
}
